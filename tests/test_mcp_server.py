from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import replace
from datetime import timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Protocol, cast

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from clio_relay import mcp_server as mcp_server_module
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    RemoteMcpServerConfig,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.identifiers import durable_record_id_json_schema
from clio_relay.mcp_server import (
    McpSessionState,
    handle_request,
    render_agent_mcp_profile,
    render_codex_mcp_profile,
    serve_stdio,
)
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpAdmissionAuthority,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    deterministic_jarvis_execution_id,
    utc_now,
)
from clio_relay.remote_mcp import (
    RemoteMcpRoute,
    RemoteMcpToolSchema,
    VirtualRemoteMcpCatalog,
    VirtualRemoteMcpTool,
    remote_mcp_registration_revision,
)
from clio_relay.spool import JobSpool


class _SchemaValidator(Protocol):
    def validate(self, instance: object) -> None:
        """Validate one JSON-compatible instance."""


def test_mcp_general_errors_remove_internal_windows_path_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Agent-facing MCP errors expose logical paths only."""
    queue = ClioCoreQueue(tmp_path / "core")
    logical_path = tmp_path / "spool" / "result.json"
    internal_path = internal_filesystem_path(logical_path, force_extended=True)

    def fail_call(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError(f"could not read {internal_path}")

    monkeypatch.setattr(
        mcp_server_module,
        "_call_tool",
        fail_call,
    )
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "relay_get_job", "arguments": {}},
        },
        queue=queue,
    )

    assert response is not None
    message = str(response["error"]["message"])
    assert "\\\\?\\" not in message
    assert str(logical_path) in message


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


def _bind_virtual_jarvis_catalog(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cluster: str,
) -> None:
    """Give a focused dispatch test one already-discovered JARVIS artifact binding."""
    original_catalog = mcp_server_module._remote_mcp_catalog  # pyright: ignore[reportPrivateUsage]

    def bound_catalog(*, profile: str, reserved_names: set[str]) -> object:
        catalog = original_catalog(profile=profile, reserved_names=reserved_names)
        return replace(
            catalog,
            jarvis_artifact_bindings={
                **catalog.jarvis_artifact_bindings,
                cluster: "a" * 64,
            },
        )

    monkeypatch.setattr(mcp_server_module, "_remote_mcp_catalog", bound_catalog)


def test_mcp_session_remote_job_routes_are_collision_safe_and_reset() -> None:
    session = McpSessionState()
    job_id = "remote-job"
    session.observe_remote_job_result(
        {
            "remote": True,
            "job_id": job_id,
            "cluster": "ares",
            "route_revision": "a" * 64,
        }
    )

    assert session.remote_job_route(job_id) == ("ares", "a" * 64)

    session.observe_remote_job_result(
        {
            "remote": True,
            "job_id": job_id,
            "cluster": "homelab",
            "route_revision": "b" * 64,
        }
    )
    with pytest.raises(ValueError, match="ambiguous in this MCP session"):
        session.remote_job_route(job_id)

    session.reset()
    assert session.remote_job_route(job_id) is None


def test_mcp_session_remote_route_never_overrides_same_id_local_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="desktop",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "local"]),
            idempotency_key="local-precedence",
        )
    )
    session = McpSessionState()
    session.observe_remote_job_result(
        {
            "remote": True,
            "job_id": job.job_id,
            "cluster": "ares",
            "route_revision": "a" * 64,
        }
    )

    class ForbiddenOwnedSessionApiClient:
        def __init__(self, **_kwargs: object) -> None:
            raise AssertionError("local job follow-up was incorrectly routed to the cluster")

    monkeypatch.setattr(
        mcp_server_module,
        "OwnedSessionApiClient",
        ForbiddenOwnedSessionApiClient,
    )
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_status",
                "arguments": {"job_id": job.job_id},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )

    assert response is not None
    structured = response["result"]["structuredContent"]
    assert structured["job"]["job_id"] == job.job_id
    assert structured["job"]["cluster"] == "desktop"


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
        "relay_bind_jarvis_runtime",
        "relay_artifact_lineage",
        "jarvis_create_pipeline",
        "jarvis_describe",
        "jarvis_add_step",
        "jarvis_edit_step",
        "jarvis_get_execution",
        "jarvis_run",
    }
    assert "jarvis_create_pipeline" in tool_names
    bind_runtime_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "relay_bind_jarvis_runtime"
    )
    assert "desktop_bind_port" not in bind_runtime_tool["inputSchema"]["properties"]
    binding_schema = bind_runtime_tool["inputSchema"]["properties"]["binding"]
    assert binding_schema["required"] == [
        "cluster",
        "source_job_id",
        "source_artifact_id",
        "package_id",
        "package_name",
        "service_instance_id",
    ]
    assert bind_runtime_tool["inputSchema"]["if"] == {"required": ["binding"]}
    assert binding_schema["properties"]["source_job_id"] == durable_record_id_json_schema()
    assert binding_schema["properties"]["source_artifact_id"] == durable_record_id_json_schema()
    bind_output_schema = bind_runtime_tool["outputSchema"]
    assert bind_output_schema["properties"]["gateway_session_id"] == {
        **durable_record_id_json_schema(),
        "pattern": r"^gateway_[0-9a-f]{32}$",
        "description": (
            "Exact relay gateway identity to pass unchanged to a viewer-opening tool. "
            "It is equal to gateway_session.session_id."
        ),
    }
    assert bind_output_schema["required"] == [
        "gateway_session_id",
        "gateway_session",
        "connect_url",
        "health_url",
        "stream_url",
        "events_url",
        "state_url",
        "command_url",
        "scheduler_cancel_requested",
    ]
    assert "top-level gateway_session_id" in bind_runtime_tool["description"]
    assert "service_instance_id is not a gateway identity" in bind_runtime_tool["description"]
    create_pipeline_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "jarvis_create_pipeline"
    )
    assert create_pipeline_tool["inputSchema"]["required"] == ["cluster", "pipeline_id"]
    assert "pipeline_id" in create_pipeline_tool["inputSchema"]["properties"]
    assert "ordinary interactive use" in create_pipeline_tool["description"]
    wait_control = create_pipeline_tool["inputSchema"]["properties"]["wait_for_terminal"]
    assert wait_control["default"] is False
    assert "current turn needs" in wait_control["description"]
    assert "never for the scheduler workload" in wait_control["description"]
    describe_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "jarvis_describe"
    )
    describe_properties = describe_tool["inputSchema"]["properties"]
    assert describe_tool["inputSchema"]["required"] == ["cluster", "target"]
    assert describe_properties["target"]["enum"] == [
        "packages",
        "package_search",
        "package",
        "pipeline",
        "step",
    ]
    assert describe_properties["page_size"]["maximum"] == 25
    assert describe_properties["query"]["anyOf"][0]["maxLength"] == 256
    assert describe_properties["cursor"]["anyOf"][0]["maxLength"] == 1024
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
    query_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "jarvis_get_execution"
    )
    query_properties = query_tool["inputSchema"]["properties"]
    assert query_properties["timeout_seconds"]["maximum"] == 60
    assert "maximum" not in run_tool["inputSchema"]["properties"]["timeout_seconds"]
    assert query_tool["inputSchema"]["required"] == [
        "cluster",
        "pipeline_id",
        "execution_id",
    ]
    assert query_properties["include_progress"] == {"default": True, "type": "boolean"}
    artifact_query = query_properties["artifacts"]["anyOf"][0]
    assert set(artifact_query["properties"]) == {
        "package_id",
        "role",
        "state",
        "artifact_id",
        "page_size",
        "cursor",
    }
    assert artifact_query["properties"]["page_size"]["maximum"] == 100
    assert query_tool["outputSchema"]["properties"]["kind"] == {
        "type": "string",
        "const": "mcp_call",
    }
    assert (
        query_tool["outputSchema"]["properties"]["service_runtime_bindings"]["items"]
        == binding_schema
    )
    diagnose_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "relay_queue_diagnose"
    )
    assert diagnose_tool["inputSchema"]["required"] == ["job_id"]
    stale_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "relay_queue_stale"
    )
    assert stale_tool["inputSchema"]["required"] == ["cluster", "older_than_seconds"]
    for name in (
        "relay_queue_list",
        "relay_queue_diagnose",
        "relay_queue_stale",
    ):
        tool = next(tool for tool in response["result"]["tools"] if tool["name"] == name)
        route_revision = tool["inputSchema"]["properties"]["route_revision"]
        assert route_revision["type"] == "string"
        assert route_revision["pattern"] == "^[0-9a-f]{64}$"
        assert "not a scientific-dataset catalog revision" in route_revision["description"]
    for name in ("relay_observe", "relay_wait"):
        log_tool = next(tool for tool in response["result"]["tools"] if tool["name"] == name)
        assert log_tool["inputSchema"]["properties"]["log_limit"]["maximum"] == 32_768
    for name in ("relay_status", "relay_cancel", "relay_observe", "relay_wait"):
        followup_tool = next(tool for tool in response["result"]["tools"] if tool["name"] == name)
        description = followup_tool["description"]
        assert "cluster, job_id, and route_revision unchanged" in description
        assert "on every follow-up call" in description
        assert "job_id alone is only for a local relay job" in description
    wait_tool = next(tool for tool in response["result"]["tools"] if tool["name"] == "relay_wait")
    assert wait_tool["inputSchema"]["properties"]["include_logs"]["default"] is False
    assert "service_runtime_bindings" in wait_tool["description"]
    assert (
        "mcp_result.structured_result as the authoritative remote tool output"
        in wait_tool["description"]
    )
    assert "do not call relay_observe merely to recover that result" in wait_tool["description"]
    assert "Never use a JARVIS execution_id as gateway_session_id" in wait_tool["description"]
    assert "JARVIS execution_id is not a gateway_session_id" in bind_runtime_tool["description"]
    assert "Never use execution_id as gateway_session_id" in query_tool["description"]
    add_step_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "jarvis_add_step"
    )
    assert "package_search is discovery only" in add_step_tool["description"]
    assert "target='package'" in add_step_tool["description"]
    assert "package-owned settings contract rather than guessing" in add_step_tool["description"]


def test_user_mcp_schemas_avoid_root_unions_and_preserve_exclusive_forms(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep agent-facing schemas visible to SDK clients without weakening validation."""

    monkeypatch.setattr(mcp_server_module, "_configured_cluster_names", lambda: ["ares"])

    response = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=ClioCoreQueue(tmp_path / "core"),
        profile="user",
    )

    assert response is not None
    tools = {tool["name"]: tool for tool in response["result"]["tools"]}
    unsafe_root_unions = {"oneOf", "anyOf", "allOf"}
    assert {
        name: sorted(unsafe_root_unions.intersection(tool["inputSchema"]))
        for name, tool in tools.items()
        if unsafe_root_unions.intersection(tool["inputSchema"])
    } == {}

    lineage = cast(
        _SchemaValidator,
        Draft202012Validator(tools["relay_artifact_lineage"]["inputSchema"]),
    )
    for accepted in (
        {"job_id": "job-source"},
        {"artifact_id": "artifact-result"},
        {
            "artifact_id": "artifact-result",
            "cluster": "ares",
            "route_revision": "a" * 64,
        },
    ):
        lineage.validate(accepted)
    for rejected in (
        {},
        {"job_id": "job-source", "artifact_id": "artifact-result"},
        {"artifact_id": "artifact-result", "cluster": "ares"},
    ):
        with pytest.raises(ValidationError):
            lineage.validate(rejected)

    handoff = {
        "cluster": "ares",
        "source_job_id": "job-source",
        "source_artifact_id": "artifact-result",
        "package_id": "paraview-1",
        "package_name": "builtin.paraview",
        "service_instance_id": "paraview-live-1",
    }
    bind_runtime = cast(
        _SchemaValidator,
        Draft202012Validator(tools["relay_bind_jarvis_runtime"]["inputSchema"]),
    )
    legacy_selectors = {
        key: value for key, value in handoff.items() if key != "service_instance_id"
    }
    for accepted in (
        {"binding": handoff},
        {"binding": handoff, "readiness_timeout_seconds": 30},
        legacy_selectors,
        {**legacy_selectors, "name": "asteroid-viewer"},
    ):
        bind_runtime.validate(accepted)
    for rejected in (
        {},
        {"binding": handoff, "cluster": "ares"},
        {key: value for key, value in legacy_selectors.items() if key != "package_name"},
        {"binding": {key: value for key, value in handoff.items() if key != "package_name"}},
    ):
        with pytest.raises(ValidationError):
            bind_runtime.validate(rejected)


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
    assert "scheduler" not in create_gateway_tool["inputSchema"]["properties"]
    assert "scheduler_job_id" not in create_gateway_tool["inputSchema"]["properties"]
    update_gateway_tool = next(
        tool
        for tool in response["result"]["tools"]
        if tool["name"] == "relay_update_gateway_session"
    )
    assert "scheduler_job_id" not in update_gateway_tool["inputSchema"]["properties"]


def test_mcp_jarvis_wait_schemas_are_explicitly_observation_only(tmp_path: Path) -> None:
    """Agent schemas must not imply that bounded observation limits scheduler work."""
    response = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=ClioCoreQueue(tmp_path / "core"),
        profile="admin",
    )

    assert response is not None
    tools = {tool["name"]: tool for tool in response["result"]["tools"]}
    relay_wait = tools["relay_wait"]
    assert "never fails, cancels, or resubmits" in relay_wait["description"]
    assert (
        "underlying relay, JARVIS, or scheduler job state"
        in (relay_wait["inputSchema"]["properties"]["timeout_seconds"]["description"])
    )
    for name in ("relay_submit_jarvis_pipeline", "relay_submit_jarvis_job"):
        tool = tools[name]
        properties = tool["inputSchema"]["properties"]
        canonical = properties["wait_timeout_seconds"]
        legacy = properties["timeout_seconds"]
        assert canonical["default"] == 600
        assert canonical["exclusiveMinimum"] == 0
        assert "Observation expiry never fails, cancels, or resubmits" in (canonical["description"])
        assert legacy["deprecated"] is True
        assert "default" not in legacy
        assert "observation-only alias" in legacy["description"]
        assert "execution deadline" in legacy["description"]
        assert (
            "never becomes a relay, JARVIS, or scheduler execution deadline"
            in (properties["wait_for_terminal"]["description"])
        )


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
    assert "result" in response, response
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


@pytest.mark.parametrize(
    ("tool_name", "source_arguments"),
    [
        (
            "relay_submit_jarvis_pipeline",
            {"pipeline_yaml": "name: observation-only\npkgs: []\n"},
        ),
        (
            "relay_submit_jarvis_job",
            {"pipeline_name": "observation-only"},
        ),
    ],
)
@pytest.mark.parametrize(
    "wait_arguments",
    [
        {"wait_timeout_seconds": 1},
        {"timeout_seconds": 1},
        {"wait_timeout_seconds": 1, "timeout_seconds": 1.0},
    ],
)
def test_mcp_jarvis_wait_timeout_never_becomes_an_execution_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tool_name: str,
    source_arguments: dict[str, object],
    wait_arguments: dict[str, object],
) -> None:
    """Agent wait bounds must not become JARVIS or scheduler runtime limits."""
    queue = ClioCoreQueue(tmp_path / "core")
    observations: list[tuple[float, float]] = []

    def bounded_wait(
        _queue: ClioCoreQueue,
        _job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> RelayJob:
        observations.append((timeout_seconds, poll_seconds))
        raise TimeoutError("bounded observation ended")

    monkeypatch.setattr(mcp_server_module, "wait_for_terminal", bounded_wait)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 120,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": {
                    "cluster": "test-cluster",
                    **source_arguments,
                    "wait_for_terminal": True,
                    **wait_arguments,
                    "poll_seconds": 0.25,
                },
            },
        },
        queue=queue,
    )

    assert response is not None and "error" not in response
    receipt = response["result"]["structuredContent"]
    assert receipt["state"] == "queued"
    assert receipt["terminal"] is False
    assert receipt["observation"] == {
        "outcome": "observation_unknown",
        "timeout_seconds": 1.0,
        "scheduler_action": "none",
        "relay_action": "none",
    }
    assert observations == [(1.0, 0.25)]
    jobs = queue.list_jobs()
    assert len(jobs) == 1
    job = jobs[0]
    assert isinstance(job.spec, JarvisRunSpec)
    assert job.spec.timeout_seconds is None
    assert job.state is JobState.QUEUED
    assert "cancellation_request" not in job.metadata


@pytest.mark.parametrize(
    ("tool_name", "source_arguments"),
    [
        (
            "relay_submit_jarvis_pipeline",
            {"pipeline_yaml": "name: conflicting-observation\npkgs: []\n"},
        ),
        (
            "relay_submit_jarvis_job",
            {"pipeline_name": "conflicting-observation"},
        ),
    ],
)
def test_mcp_jarvis_wait_timeout_alias_conflict_fails_before_submission(
    tmp_path: Path,
    tool_name: str,
    source_arguments: dict[str, object],
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 121,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": {
                    "cluster": "test-cluster",
                    **source_arguments,
                    "wait_for_terminal": True,
                    "wait_timeout_seconds": 1,
                    "timeout_seconds": 2,
                },
            },
        },
        queue=queue,
    )

    assert response is not None and "error" in response
    assert response["error"]["message"] == (
        "wait_timeout_seconds and legacy timeout_seconds must be equal when both are "
        "provided; both fields bound observation only"
    )
    assert queue.list_jobs() == []


@pytest.mark.parametrize(
    ("tool_name", "source_arguments", "path", "source_key"),
    [
        (
            "relay_submit_jarvis_pipeline",
            {"pipeline_yaml": "name: owned-observation\npkgs: []\n"},
            "/jobs/jarvis",
            "pipeline_yaml",
        ),
        (
            "relay_submit_jarvis_job",
            {"pipeline_name": "owned-observation"},
            "/jobs/jarvis-pipeline",
            "pipeline_name",
        ),
    ],
)
def test_owned_mcp_jarvis_wait_uses_only_the_canonical_observation_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tool_name: str,
    source_arguments: dict[str, object],
    path: str,
    source_key: str,
) -> None:
    definition = ClusterDefinition(name="cluster-a", ssh_host="cluster-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"cluster-a": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    captured: dict[str, object] = {}

    def submit_owned(**kwargs: object) -> RelayJob:
        captured["submission"] = kwargs
        payload = cast(dict[str, object], kwargs["payload"])
        source_value = cast(str, payload[source_key])
        spec = (
            JarvisRunSpec(pipeline_yaml=source_value)
            if source_key == "pipeline_yaml"
            else JarvisRunSpec(pipeline_name=source_value)
        )
        return RelayJob(
            cluster="cluster-a",
            kind=JobKind.JARVIS,
            spec=spec,
            idempotency_key=cast(str, payload["idempotency_key"]),
            metadata={
                "owner_session_id": "desktop-session-1",
                "owner_session_generation_id": "generation-1",
            },
        )

    def submission_result(job: RelayJob, **kwargs: object) -> dict[str, object]:
        captured["result"] = kwargs
        captured["job"] = job
        return {"job_id": job.job_id, "state": job.state.value}

    monkeypatch.setattr(mcp_server_module, "submit_owned_session_job", submit_owned)
    monkeypatch.setattr(
        mcp_server_module,
        "_owned_session_submission_result",
        submission_result,
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
    )

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 122,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": {
                    "cluster": "cluster-a",
                    **source_arguments,
                    "wait_for_terminal": True,
                    "wait_timeout_seconds": 1,
                    "timeout_seconds": 1.0,
                    "poll_seconds": 0.25,
                },
            },
        },
        queue=ClioCoreQueue(settings.core_dir),
        settings=settings,
        profile="admin",
    )

    assert response is not None and "error" not in response, response
    submission = cast(dict[str, object], captured["submission"])
    payload = cast(dict[str, object], submission["payload"])
    result = cast(dict[str, object], captured["result"])
    job = cast(RelayJob, captured["job"])
    assert submission["path"] == path
    assert "timeout_seconds" not in payload
    assert "wait_timeout_seconds" not in payload
    assert result["wait_timeout_seconds"] == 1.0
    assert result["poll_seconds"] == 0.25
    assert isinstance(job.spec, JarvisRunSpec)
    assert job.spec.timeout_seconds is None


def test_owned_mcp_jarvis_wait_deadline_reobserves_the_same_remote_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An owned-session response deadline must become a resumable observation result."""
    definition = ClusterDefinition(name="cluster-a", ssh_host="cluster-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"cluster-a": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
    )
    remote_job = RelayJob(
        cluster="cluster-a",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_name="long-owned-run"),
        idempotency_key="long-owned-run",
        metadata={
            "owner": "clio-relay",
            "owner_session_id": settings.owner_session_id,
            "owner_session_generation_id": settings.owner_session_generation_id,
        },
    )

    def submit_owned(**_kwargs: object) -> RelayJob:
        return remote_job

    monkeypatch.setattr(mcp_server_module, "submit_owned_session_job", submit_owned)
    requests: list[tuple[str, str]] = []

    class FakeOwnedSessionApiClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> FakeOwnedSessionApiClient:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def request_json(
            self,
            *,
            method: str,
            path: str,
            query: dict[str, object] | None = None,
            body: dict[str, object] | None = None,
            response_timeout_seconds: float | None = None,
        ) -> object:
            del query, body, response_timeout_seconds
            requests.append((method, path))
            if method == "POST" and path == f"/jobs/{remote_job.job_id}/wait":
                raise ObservationTimeoutError("owned wait response deadline expired")
            if method == "GET" and path == f"/jobs/{remote_job.job_id}/status":
                return {
                    "job": remote_job.model_dump(mode="json"),
                    "relay_queue": {},
                    "scheduler": [],
                    "terminal": False,
                }
            raise AssertionError(f"unexpected request: {method} {path}")

    monkeypatch.setattr(
        mcp_server_module,
        "OwnedSessionApiClient",
        FakeOwnedSessionApiClient,
    )
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 125,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_job",
                "arguments": {
                    "cluster": "cluster-a",
                    "pipeline_name": "long-owned-run",
                    "wait_for_terminal": True,
                    "wait_timeout_seconds": 0.25,
                    "poll_seconds": 0.05,
                },
            },
        },
        queue=ClioCoreQueue(settings.core_dir),
        settings=settings,
        profile="admin",
    )

    assert response is not None and "error" not in response
    receipt = response["result"]["structuredContent"]
    assert receipt["job_id"] == remote_job.job_id
    assert receipt["state"] == "queued"
    assert receipt["terminal"] is False
    assert receipt["observation"]["outcome"] == "observation_unknown"
    assert receipt["observation"]["scheduler_action"] == "none"
    assert requests == [
        ("POST", f"/jobs/{remote_job.job_id}/wait"),
        ("GET", f"/jobs/{remote_job.job_id}/status"),
    ]


def test_direct_remote_named_jarvis_wait_is_rejected_before_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="cluster-a", ssh_host="cluster-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"cluster-a": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    commands: list[list[str]] = []

    def run_remote(_definition: ClusterDefinition, arguments: list[str]) -> str:
        commands.append(arguments)
        return "job_1234567890abcdef1234567890abcdef\n"

    monkeypatch.setattr(mcp_server_module, "run_remote_clio", run_remote)
    queue = ClioCoreQueue(tmp_path / "core")
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 123,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_job",
                "arguments": {
                    "cluster": "cluster-a",
                    "pipeline_name": "durable-pipeline",
                    "wait_for_terminal": True,
                    "wait_timeout_seconds": 1,
                },
            },
        },
        queue=queue,
        profile="admin",
    )

    assert response is not None and "error" in response
    assert "submit asynchronously" in response["error"]["message"]
    assert "call relay_wait" in response["error"]["message"]
    assert commands == []
    assert queue.list_jobs() == []

    asynchronous = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 124,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_job",
                "arguments": {
                    "cluster": "cluster-a",
                    "pipeline_name": "durable-pipeline",
                    "wait_for_terminal": False,
                },
            },
        },
        queue=queue,
        profile="admin",
    )
    assert asynchronous is not None and "error" not in asynchronous
    receipt = asynchronous["result"]["structuredContent"]
    assert receipt["job_id"] == "job_1234567890abcdef1234567890abcdef"
    assert receipt["terminal"] is False
    assert commands[0][:2] == ["job", "submit-pipeline"]


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
                "arguments": {
                    "job_id": job.job_id,
                    "timeout_seconds": 1,
                    "include_logs": True,
                },
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
    assert waited["observation"]["outcome"] == "terminal"
    assert "finished" in waited["logs"]["stdout"]["text"]
    assert cancel_response is not None
    assert cancel_response["result"]["structuredContent"]["job_id"] == job.job_id


def test_mcp_wait_omits_logs_unless_explicitly_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terminal structured results do not carry stdout and stderr by default."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="wait-without-logs",
        )
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED, message="done")

    def reject_log_read(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("relay_wait read logs without include_logs=true")

    monkeypatch.setattr(mcp_server_module, "_job_logs", reject_log_read)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 32,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {"job_id": job.job_id, "timeout_seconds": 1},
            },
        },
        queue=queue,
        settings=settings,
    )

    assert response is not None and "error" not in response
    waited = response["result"]["structuredContent"]
    assert waited["terminal"] is True
    assert waited["observation"]["outcome"] == "terminal"
    assert "logs" not in waited


def test_mcp_wait_timeout_returns_the_same_durable_job_without_actions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bounded observation ending must return a resumable receipt, not fail the job."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="long-queued-run"),
            idempotency_key="durable-wait-observation",
        )
    )

    def bounded_wait(
        _queue: ClioCoreQueue,
        _job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> RelayJob:
        assert timeout_seconds == 0.25
        assert poll_seconds == 0.05
        raise TimeoutError("bounded observation ended")

    monkeypatch.setattr(mcp_server_module, "wait_for_terminal", bounded_wait)
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 33,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {
                    "job_id": job.job_id,
                    "timeout_seconds": 0.25,
                    "poll_seconds": 0.05,
                },
            },
        },
        queue=queue,
        settings=settings,
    )

    assert response is not None and "error" not in response
    observed = response["result"]["structuredContent"]
    assert observed["job"]["job_id"] == job.job_id
    assert observed["job"]["state"] == "queued"
    assert observed["terminal"] is False
    assert observed["observation"] == {
        "outcome": "observation_unknown",
        "timeout_seconds": 0.25,
        "scheduler_action": "none",
        "relay_action": "none",
    }
    preserved = queue.get_job(job.job_id)
    assert preserved.state is JobState.QUEUED
    assert "cancellation_request" not in preserved.metadata


def test_mcp_wait_classifies_the_exact_status_snapshot_after_timeout_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job completing at the observation boundary must not be reported as unknown."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="boundary-completion"),
            idempotency_key="boundary-completion",
        )
    )

    def complete_at_boundary(
        selected_queue: ClioCoreQueue,
        job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> RelayJob:
        del timeout_seconds, poll_seconds
        selected_queue.update_job_state(job_id, JobState.SUCCEEDED)
        raise TimeoutError("observation boundary raced with completion")

    monkeypatch.setattr(mcp_server_module, "wait_for_terminal", complete_at_boundary)
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 331,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {
                    "job_id": job.job_id,
                    "timeout_seconds": 0.25,
                    "poll_seconds": 0.05,
                },
            },
        },
        queue=queue,
        settings=settings,
    )

    assert response is not None and "error" not in response
    observed = response["result"]["structuredContent"]
    assert observed["job"]["state"] == "succeeded"
    assert observed["terminal"] is True
    assert observed["observation"]["outcome"] == "terminal"


def test_direct_remote_wait_timeout_reobserves_exact_receipt_without_actions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SSH wait expiry must fall through to an exact status read, never a resubmission."""
    definition = ClusterDefinition(name="cluster-a", ssh_host="cluster-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"cluster-a": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    remote_job = RelayJob(
        cluster="cluster-a",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_name="long-remote-run"),
        idempotency_key="long-remote-run",
    )
    commands: list[list[str]] = []

    def run_remote(_definition: ClusterDefinition, arguments: list[str]) -> str:
        commands.append(arguments)
        if arguments[:2] == ["job", "wait"]:
            raise ObservationTimeoutError("remote observation deadline expired")
        if arguments[:2] == ["job", "status"]:
            return json.dumps(
                {
                    "job": remote_job.model_dump(mode="json"),
                    "relay_queue": {},
                    "scheduler": [{"state": "PENDING", "job_id": "slurm-42"}],
                    "terminal": False,
                }
            )
        raise AssertionError(f"unexpected remote command: {arguments}")

    monkeypatch.setattr(mcp_server_module, "run_remote_clio", run_remote)
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 34,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {
                    "cluster": definition.name,
                    "job_id": remote_job.job_id,
                    "route_revision": cluster_route_revision(definition),
                    "timeout_seconds": 0.25,
                    "poll_seconds": 0.05,
                },
            },
        },
        queue=ClioCoreQueue(tmp_path / "core"),
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        profile="admin",
    )

    assert response is not None and "error" not in response
    observed = response["result"]["structuredContent"]
    assert observed["job"]["job_id"] == remote_job.job_id
    assert observed["job"]["state"] == "queued"
    assert observed["scheduler"] == [{"state": "PENDING", "job_id": "slurm-42"}]
    assert observed["terminal"] is False
    assert observed["observation"]["outcome"] == "observation_unknown"
    assert commands == [
        [
            "job",
            "wait",
            remote_job.job_id,
            "--timeout-seconds",
            "0.25",
            "--poll-seconds",
            "0.05",
        ],
        ["job", "status", remote_job.job_id],
    ]

    rejected_commands: list[list[str]] = []

    def reject_wait(_definition: ClusterDefinition, arguments: list[str]) -> str:
        rejected_commands.append(arguments)
        raise RelayError("remote authentication rejected the wait")

    monkeypatch.setattr(mcp_server_module, "run_remote_clio", reject_wait)
    rejected = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 35,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {
                    "cluster": definition.name,
                    "job_id": remote_job.job_id,
                    "route_revision": cluster_route_revision(definition),
                    "timeout_seconds": 0.25,
                    "poll_seconds": 0.05,
                },
            },
        },
        queue=ClioCoreQueue(tmp_path / "rejected-core"),
        settings=RelaySettings(
            core_dir=tmp_path / "rejected-core",
            spool_dir=tmp_path / "rejected-spool",
        ),
        profile="admin",
    )

    assert rejected is not None and "error" in rejected
    assert "authentication rejected" in rejected["error"]["message"]
    assert len(rejected_commands) == 1
    assert rejected_commands[0][:2] == ["job", "wait"]


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
    assert response["error"]["message"] == "log_limit must be between 1 and 32768"


def test_mcp_observe_bounds_broad_regex_matches_and_log_context(tmp_path: Path) -> None:
    """A broad agent-authored regex cannot duplicate an entire large log response."""

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="bounded-broad-observe",
        )
    )
    spool = JobSpool(settings.spool_dir, job)
    spool.initialize()
    spool.append_stdout("x" * 50_000)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 34,
            "method": "tools/call",
            "params": {
                "name": "relay_observe",
                "arguments": {
                    "job_id": job.job_id,
                    "pattern": ".+",
                    "log_limit": 32_768,
                },
            },
        },
        queue=queue,
        settings=settings,
    )

    assert response is not None and "error" not in response
    observed = response["result"]["structuredContent"]
    assert observed["matched"] is True
    assert len(observed["logs"]["stdout"]["text"]) == 32_768
    stdout_match = next(item for item in observed["matches"] if item.get("source") == "stdout")
    assert len(stdout_match["text"]) <= 1_024
    assert len(stdout_match["match"]) <= 1_024
    assert stdout_match["match_truncated"] is True
    assert len(json.dumps(observed)) < 40_000


def test_mcp_job_route_rejects_missing_or_catalog_revisions_before_remote_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remote job handles require the exact opaque route token from their receipt."""

    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))

    def remote_io_forbidden(_definition: ClusterDefinition, _args: list[str]) -> str:
        raise AssertionError("invalid route identity reached remote I/O")

    monkeypatch.setattr(mcp_server_module, "run_remote_clio", remote_io_forbidden)
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")

    missing = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 35,
            "method": "tools/call",
            "params": {
                "name": "relay_status",
                "arguments": {"cluster": "ares", "job_id": "job_remote_1"},
            },
        },
        queue=queue,
        settings=settings,
    )
    catalog_revision = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 36,
            "method": "tools/call",
            "params": {
                "name": "relay_status",
                "arguments": {
                    "cluster": "ares",
                    "job_id": "job_remote_1",
                    "route_revision": "2026-07-15-live-demo-v1",
                },
            },
        },
        queue=queue,
        settings=settings,
    )

    assert missing is not None
    assert "route_revision is required" in missing["error"]["message"]
    assert catalog_revision is not None
    assert "64-character lowercase hexadecimal token" in catalog_revision["error"]["message"]


def test_mcp_compact_job_handle_routes_remote_lifecycle_and_verifies_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "clusters.json"
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    queue = ClioCoreQueue(tmp_path / "desktop-core")
    settings = RelaySettings(core_dir=tmp_path / "desktop-core", spool_dir=tmp_path / "spool")
    job_id = "remote-job-1"
    remote_job = RelayJob(
        job_id=job_id,
        cluster="ares",
        kind=JobKind.MCP_CALL,
        state=JobState.SUCCEEDED,
        spec=McpCallSpec(server="science", tool="inspect"),
        idempotency_key="remote-science-inspect",
    )
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
                    "job": remote_job.model_dump(mode="json"),
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
                "arguments": {
                    "cluster": "ares",
                    "route_revision": cluster_route_revision(definition),
                    "job_id": job_id,
                },
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
                    "node": "compute-01",
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


@pytest.mark.parametrize(
    "forged",
    [
        {"scheduler": "slurm"},
        {"scheduler_job_id": "12345"},
        {"gateway": {"runtime_spec": {"kind": "forged"}}},
        {"gateway": {"jarvis_runtime_binding": {"schema_version": "forged"}}},
        {"gateway": {"ownership_intents": {"scheduler_submission": {}}}},
        {"gateway": {"scheduler_provider": "slurm"}},
        {"gateway": {"scheduler_job_id": "12345"}},
        {"gateway": {"scheduler_native_id": "12345"}},
        {"gateway": {"transport": {"remote_connector": {"pid": 42}}}},
        {"metadata": {"owner": "clio-relay"}},
        {"metadata": {"scheduler_provider": "slurm"}},
        {"metadata": {"scheduler_native_id": "12345"}},
    ],
)
def test_mcp_generic_gateway_create_rejects_runtime_ownership_fields(
    tmp_path: Path,
    forged: dict[str, object],
) -> None:
    """The admin convenience tool cannot forge supervisor-owned runtime identity."""
    queue = ClioCoreQueue(tmp_path / "core")
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 240,
            "method": "tools/call",
            "params": {
                "name": "relay_create_gateway_session",
                "arguments": {
                    "cluster": "target-cluster",
                    "name": "forged-runtime",
                    **forged,
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    assert "relay-managed runtime fields" in response["error"]["message"]
    assert queue.list_gateway_sessions() == []


@pytest.mark.parametrize(
    "forged",
    [
        {"scheduler_job_id": "12345"},
        {"gateway": {"runtime_spec": {"kind": "forged"}}},
        {"gateway": {"jarvis_runtime_binding": {"schema_version": "forged"}}},
        {"gateway": {"scheduler_provider": "slurm"}},
        {"gateway": {"scheduler_job_id": "12345"}},
        {"gateway": {"scheduler_native_id": "12345"}},
        {"gateway": {"transport": {"desktop_connector": {"pid": 42}}}},
        {"metadata": {"owner_session_id": "forged-session"}},
        {"metadata": {"scheduler_job_id": "12345"}},
    ],
)
def test_mcp_generic_gateway_update_rejects_runtime_ownership_fields(
    tmp_path: Path,
    forged: dict[str, object],
) -> None:
    """Generic updates retain ordinary fields but reject runtime ownership mutations."""
    queue = ClioCoreQueue(tmp_path / "core")
    session = queue.create_gateway_session(
        GatewaySession(cluster="target-cluster", name="ordinary-gateway")
    )
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 241,
            "method": "tools/call",
            "params": {
                "name": "relay_update_gateway_session",
                "arguments": {"session_id": session.session_id, **forged},
            },
        },
        queue=queue,
    )

    assert response is not None
    assert "relay-managed runtime fields" in response["error"]["message"]
    assert queue.get_gateway_session(session.session_id) == session


def test_mcp_generic_gateway_update_cannot_replace_owned_runtime_state(tmp_path: Path) -> None:
    """A benign-looking replacement cannot erase an existing relay runtime anchor."""
    queue = ClioCoreQueue(tmp_path / "core")
    runtime = queue.create_gateway_session(
        GatewaySession(
            cluster="target-cluster",
            name="owned-runtime",
            gateway={
                "runtime_spec": {"kind": "image-service"},
                "ownership_intents": {"scheduler_submission": {"state": "recorded"}},
            },
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 242,
            "method": "tools/call",
            "params": {
                "name": "relay_update_gateway_session",
                "arguments": {
                    "session_id": runtime.session_id,
                    "gateway": {"strategy": "ssh_forward"},
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    assert "cannot replace relay-managed runtime state" in response["error"]["message"]
    assert queue.get_gateway_session(runtime.session_id).gateway == runtime.gateway


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


def test_generic_mcp_default_idempotency_key_excludes_builtin_jarvis_marker(
    tmp_path: Path,
) -> None:
    """Keep generic MCP call identity stable when the built-in marker is absent."""
    queue = ClioCoreQueue(tmp_path / "core")
    tool_arguments = {"case": "site-simulation", "steps": 100}
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 221,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_mcp_call",
                "arguments": {
                    "cluster": "test-cluster",
                    "server": "remote-tool-server",
                    "server_args": ["--stdio"],
                    "tool": "run",
                    "arguments": tool_arguments,
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    job = queue.get_job(response["result"]["structuredContent"]["job_id"])
    argument_digest = hashlib.sha256(
        json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    expected_identity: dict[str, object] = {
        "cluster": "test-cluster",
        "server": "remote-tool-server",
        "server_args": ["--stdio"],
        "env_from": {},
        "expected_server_artifact_digest": None,
        "tool": "run",
        "arguments_digest": argument_digest,
        "timeout_seconds": None,
    }
    assert job.idempotency_key == (
        "mcp:mcp-call:"
        + mcp_server_module._stable_digest(  # pyright: ignore[reportPrivateUsage]
            expected_identity
        )
    )


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
    assert job.spec.server == "clio-kit"
    assert job.spec.server_args == ["mcp-server", "jarvis"]
    assert job.spec.tool == "jarvis_describe"
    assert job.spec.arguments == {"target": "packages"}
    assert job.spec.expected_jarvis_cd_lock_binding == (
        mcp_server_module.jarvis_cd_lock_binding_expectation()
    )


def test_mcp_virtual_jarvis_tool_routes_to_cluster_mcp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_cluster(tmp_path, monkeypatch, "ares")
    _bind_virtual_jarvis_catalog(monkeypatch, cluster="ares")
    queue = ClioCoreQueue(tmp_path / "core")
    session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 23, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert listed is not None
    advertised = next(
        tool for tool in listed["result"]["tools"] if tool["name"] == "jarvis_create_pipeline"
    )
    advertised_revision = listed["result"]["_meta"]["clio-relay/remote-mcp-catalog-revision"]

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
        profile="user",
        session=session,
    )

    assert response is not None
    assert "result" in response, response
    result = response["result"]["structuredContent"]
    cast(_SchemaValidator, Draft202012Validator(advertised["outputSchema"])).validate(result)
    assert result["catalog_revision"] == advertised_revision
    assert result["catalog_revision"] == session.observed_remote_mcp_catalog_revision(
        profile="user"
    )
    job = queue.get_job(result["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.cluster == "ares"
    assert job.spec.server == "clio-kit"
    assert job.spec.server_args == ["mcp-server", "jarvis"]
    assert job.spec.tool == "jarvis_create_pipeline"
    assert job.spec.arguments == {"pipeline_id": "site_simulation_4node"}


def test_remote_virtual_jarvis_call_defers_artifact_selection_to_cluster(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    _bind_virtual_jarvis_catalog(monkeypatch, cluster="ares")
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
    session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 239, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert listed is not None
    advertised = next(
        tool for tool in listed["result"]["tools"] if tool["name"] == "jarvis_describe"
    )
    advertised_revision = listed["result"]["_meta"]["clio-relay/remote-mcp-catalog-revision"]

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 240,
            "method": "tools/call",
            "params": {
                "name": "jarvis_describe",
                "arguments": {
                    "cluster": "ares",
                    "target": "package_search",
                    "query": "parallel visualization",
                    "page_size": 7,
                    "idempotency_key": "remote-receipt-bound-jarvis",
                },
            },
        },
        queue=queue,
        profile="user",
        session=session,
    )

    assert response is not None
    structured = response["result"]["structuredContent"]
    cast(_SchemaValidator, Draft202012Validator(advertised["outputSchema"])).validate(structured)
    assert structured["job_id"] == "job_remote_jarvis"
    assert structured["catalog_revision"] == advertised_revision
    assert structured["catalog_revision"] == session.observed_remote_mcp_catalog_revision(
        profile="user"
    )
    assert writes and json.loads(writes[0][1]) == {
        "target": "package_search",
        "query": "parallel visualization",
        "page_size": 7,
    }
    assert removals == [writes[0][0]]
    assert commands[0][0] == "jarvis-mcp-call"
    assert "--server" not in commands[0]
    assert commands[0][commands[0].index("--tool") + 1] == "jarvis_describe"


def test_owned_remote_virtual_jarvis_call_uses_authenticated_session_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    _bind_virtual_jarvis_catalog(monkeypatch, cluster="ares")

    def artifact_binding(_cluster: str) -> str:
        return "a" * 64

    monkeypatch.setattr(
        "clio_relay.mcp_server.jarvis_mcp_artifact_binding",
        artifact_binding,
    )
    captured: dict[str, object] = {}

    def submit_owned(**kwargs: object) -> RelayJob:
        captured.update(kwargs)
        payload = cast(dict[str, object], kwargs["payload"])
        selected_settings = cast(RelaySettings, kwargs["settings"])
        return RelayJob(
            cluster="ares",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server="clio-kit",
                server_args=["mcp-server", "jarvis"],
                expected_server_artifact_digest=cast(
                    str,
                    payload["expected_server_artifact_digest"],
                ),
                tool=cast(str, payload["tool"]),
                arguments=cast(dict[str, object], payload["arguments"]),
            ),
            idempotency_key=cast(str, payload["idempotency_key"]),
            metadata={
                "owner": "clio-relay",
                "owner_session_id": selected_settings.owner_session_id,
                "owner_session_generation_id": (selected_settings.owner_session_generation_id),
            },
        )

    monkeypatch.setattr("clio_relay.mcp_server.submit_owned_session_job", submit_owned)

    def fail_remote(_definition: ClusterDefinition, _args: list[str]) -> str:
        raise AssertionError("owned virtual call bypassed the session API")

    monkeypatch.setattr("clio_relay.mcp_server.run_remote_clio", fail_remote)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="ares",
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )
    assert listed is not None

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "jarvis_get_execution",
                "arguments": {
                    "cluster": "ares",
                    "pipeline_id": "pipeline",
                    "execution_id": "execution-1",
                    "include_service_runtimes": True,
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )

    assert response is not None
    assert "result" in response, response
    result = response["result"]["structuredContent"]
    assert result["remote"] is True
    assert captured["path"] == "/jobs/jarvis-mcp-call"
    payload = cast(dict[str, object], captured["payload"])
    assert payload["expected_server_artifact_digest"] == "a" * 64
    assert payload["arguments"] == {
        "pipeline_id": "pipeline",
        "execution_id": "execution-1",
        "include_service_runtimes": True,
    }
    assert queue.list_jobs() == []


def test_waited_owned_jarvis_call_returns_bounded_artifact_bound_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A waited virtual call returns its structured error without a second relay query."""

    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    _bind_virtual_jarvis_catalog(monkeypatch, cluster="ares")

    def artifact_binding(_cluster: str) -> str:
        return "a" * 64

    monkeypatch.setattr(
        "clio_relay.mcp_server.jarvis_mcp_artifact_binding",
        artifact_binding,
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="ares",
    )
    queued = RelayJob(
        cluster="ares",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server="clio-kit",
            server_args=["mcp-server", "jarvis"],
            expected_server_artifact_digest="a" * 64,
            tool="jarvis_run",
            arguments={"pipeline_id": "simulation"},
        ),
        idempotency_key="waited-owned-jarvis-failure",
        metadata={
            "owner": "clio-relay",
            "owner_session_id": "desktop-session-1",
            "owner_session_generation_id": "generation-1",
        },
    )
    terminal = queued.model_copy(update={"state": JobState.FAILED, "last_error": "exit code 1"})
    payload = json.dumps(
        {
            "operation": "tools/call",
            "tool": "jarvis_run",
            "returncode": 1,
            "timed_out": False,
            "protocol_error": None,
            "structured_result": {
                "schema_version": "jarvis.error.v1",
                "error": {
                    "code": "jarvis_run_failed",
                    "message": "site software resolution failed",
                },
            },
            "protocol_result": {"isError": True},
            "protocol_version": "2024-11-05",
            "server_info": {"name": "jarvis"},
            "result_validation": None,
        },
        sort_keys=True,
    ).encode()
    artifact = {
        "artifact_id": "artifact_waited_result",
        "job_id": queued.job_id,
        "kind": "mcp_result",
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "created_at": "2026-07-16T12:38:30Z",
    }
    requests: list[tuple[str, str, float | None]] = []

    class FakeOwnedSessionApiClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> FakeOwnedSessionApiClient:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def request_json(
            self,
            *,
            method: str,
            path: str,
            query: dict[str, object] | None = None,
            body: dict[str, object] | None = None,
            response_timeout_seconds: float | None = None,
        ) -> object:
            del query, body
            requests.append((method, path, response_timeout_seconds))
            if path == f"/jobs/{queued.job_id}/wait":
                return {
                    **terminal.model_dump(mode="json"),
                    "observation": {
                        "outcome": "terminal",
                        "timeout_seconds": 600,
                        "scheduler_action": "none",
                        "relay_action": "none",
                    },
                }
            if path == f"/jobs/{queued.job_id}/artifacts":
                return {
                    "artifacts": [artifact],
                    "cursor": 1,
                    "limit": 500,
                    "next_cursor": None,
                    "total": 1,
                }
            if path == f"/artifacts/{artifact['artifact_id']}/content":
                return {
                    "artifact": artifact,
                    "encoding": "base64",
                    "data": base64.b64encode(payload).decode("ascii"),
                }
            raise AssertionError(f"unexpected owned request: {method} {path}")

    def submit_owned(**_kwargs: object) -> RelayJob:
        return queued

    monkeypatch.setattr(mcp_server_module, "submit_owned_session_job", submit_owned)
    monkeypatch.setattr(
        mcp_server_module,
        "OwnedSessionApiClient",
        FakeOwnedSessionApiClient,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )
    assert listed is not None
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "jarvis_run",
                "arguments": {
                    "cluster": "ares",
                    "pipeline_id": "simulation",
                    "wait_for_terminal": True,
                    "wait_timeout_seconds": 600,
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )

    assert response is not None and "error" not in response
    result = response["result"]["structuredContent"]
    advertised = next(tool for tool in listed["result"]["tools"] if tool["name"] == "jarvis_run")
    cast(_SchemaValidator, Draft202012Validator(advertised["outputSchema"])).validate(result)
    assert result["state"] == "failed"
    assert result["last_error"] == "exit code 1"
    assert result["mcp_result"]["structured_result"]["error"]["code"] == ("jarvis_run_failed")
    assert result["mcp_result_artifact"]["artifact_id"] == artifact["artifact_id"]
    assert requests == [
        ("POST", f"/jobs/{queued.job_id}/wait", 610),
        ("GET", f"/jobs/{queued.job_id}/artifacts", None),
        ("GET", f"/artifacts/{artifact['artifact_id']}/content", None),
    ]


def test_direct_remote_waited_mcp_submission_returns_artifact_bound_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The SSH fallback returns terminal MCP evidence in the original receipt."""

    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    job_id = "job_direct_waited_1"
    payload = json.dumps(
        {
            "operation": "tools/call",
            "tool": "jarvis_describe",
            "returncode": 0,
            "timed_out": False,
            "protocol_error": None,
            "structured_result": {"package": "builtin.paraview"},
            "protocol_result": {"structuredContent": {"package": "builtin.paraview"}},
            "protocol_version": "2024-11-05",
            "server_info": {"name": "jarvis"},
            "result_validation": None,
        },
        sort_keys=True,
    ).encode()
    artifact = {
        "artifact_id": "artifact_direct_waited_result",
        "job_id": job_id,
        "kind": "mcp_result",
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "created_at": "2026-07-16T12:45:00Z",
    }
    commands: list[list[str]] = []

    def run_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        commands.append(args)
        if args[:2] == ["job", "wait"]:
            return ""
        if args[:2] == ["job", "status"]:
            return json.dumps(
                {
                    "job": {
                        "job_id": job_id,
                        "cluster": "ares",
                        "kind": "mcp_call",
                        "state": "succeeded",
                        "spec": {
                            "server": "jarvis",
                            "tool": "jarvis_describe",
                        },
                        "idempotency_key": "direct-waited-fixture",
                        "last_error": None,
                    },
                    "terminal": True,
                }
            )
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
                    "data": base64.b64encode(payload).decode("ascii"),
                }
            )
        raise AssertionError(f"unexpected remote command: {args}")

    monkeypatch.setattr(mcp_server_module, "run_remote_clio", run_remote)

    result = mcp_server_module._remote_mcp_submission_result(  # pyright: ignore[reportPrivateUsage]
        f"{job_id}\n",
        definition=definition,
        arguments={
            "wait_for_terminal": True,
            "wait_timeout_seconds": 30,
            "poll_seconds": 0.25,
        },
    )

    assert result["state"] == "succeeded"
    assert result["terminal"] is True
    assert result["last_error"] is None
    assert result["mcp_result"]["structured_result"] == {"package": "builtin.paraview"}
    assert result["mcp_result"]["protocol_result_omitted"] == ("redundant_with_structured_result")
    assert "protocol_result" not in result["mcp_result"]
    assert result["mcp_result_artifact"]["artifact_id"] == artifact["artifact_id"]
    assert [command[:2] for command in commands] == [
        ["job", "wait"],
        ["job", "status"],
        ["job", "list-artifacts"],
        ["job", "read-artifact"],
    ]


def test_under_limit_terminal_mcp_result_returns_complete_structured_result() -> None:
    """A safely bounded result remains a successful, complete agent result."""

    bounded = mcp_server_module._bounded_mcp_result(  # pyright: ignore[reportPrivateUsage]
        {
            "operation": "tools/call",
            "tool": "science_inspect",
            "returncode": 0,
            "timed_out": False,
            "protocol_error": None,
            "structured_result": {"dataset_id": "asteroid-first-five"},
            "protocol_result": {"structuredContent": {"dataset_id": "asteroid-first-five"}},
        }
    )

    assert bounded["structured_result"] == {"dataset_id": "asteroid-first-five"}
    assert bounded["protocol_result_omitted"] == "redundant_with_structured_result"
    assert "delivery" not in bounded


def test_oversized_terminal_mcp_result_fails_closed_without_partial_payload() -> None:
    """Oversized arbitrary output becomes an explicit, secret-free delivery failure."""

    secret = "unclassified-application-secret-" + "x" * 100_000

    bounded = mcp_server_module._bounded_mcp_result(  # pyright: ignore[reportPrivateUsage]
        {
            "operation": "tools/call",
            "tool": "science_inspect",
            "returncode": 0,
            "timed_out": False,
            "protocol_error": None,
            "structured_result": {"application_payload": secret},
            "protocol_result": {"structuredContent": {"application_payload": secret}},
            "protocol_version": "2024-11-05",
            "server_info": {"name": "science"},
            "result_validation": None,
        }
    )

    assert bounded == {
        "content_truncated": True,
        "result_available": False,
        "delivery": {
            "schema_version": "clio-relay.mcp-result-delivery.v1",
            "status": "failed",
            "code": "inline_result_limit_exceeded",
            "max_inline_bytes": 65_536,
            "private_evidence_preserved": True,
            "remote_side_effects_may_have_occurred": True,
            "message": mcp_server_module.MCP_RESULT_INLINE_LIMIT_MESSAGE,
        },
    }
    assert "structured_result" not in bounded
    assert "protocol_result" not in bounded
    assert secret not in json.dumps(bounded, sort_keys=True)


def test_oversized_terminal_mcp_result_sets_tool_error_and_preserves_job_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The MCP call fails while preserving truthful remote state and private evidence."""

    secret = "opaque-result-secret-" + "z" * 100_000
    document: dict[str, Any] = {
        "operation": "tools/call",
        "tool": "science_inspect",
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
        "structured_result": {"application_payload": secret},
    }
    source_job = RelayJob(
        job_id="job_oversized_remote_result",
        cluster="ares",
        kind=JobKind.MCP_CALL,
        state=JobState.SUCCEEDED,
        spec=McpCallSpec(server="science-mcp", tool="science_inspect"),
        idempotency_key="oversized-result-fixture",
    )
    payload = json.dumps(document, sort_keys=True).encode("utf-8")
    artifact = ArtifactRef(
        artifact_id="artifact_oversized_remote_result",
        job_id=source_job.job_id,
        uri=(tmp_path / "private-mcp-result.json").as_uri(),
        kind="mcp_result",
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    receipt: dict[str, Any] = {
        "cluster": "ares",
        "job_id": source_job.job_id,
        "state": "succeeded",
        "kind": "mcp_call",
        "terminal": True,
        "remote": True,
        "route_revision": "a" * 64,
    }
    parsed = mcp_server_module._VerifiedMcpResult(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        document=document,
        public=document,
    )
    mcp_server_module._attach_terminal_mcp_evidence(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        source_job=source_job,
        last_error=None,
        artifacts=[artifact.model_dump(mode="json")],
        parsed_result=parsed,
    )

    def waited_result(
        _arguments: dict[str, Any],
        *,
        queue: ClioCoreQueue,
        settings: RelaySettings,
    ) -> dict[str, Any]:
        del queue, settings
        return receipt

    monkeypatch.setattr(mcp_server_module, "_wait_job", waited_result)
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {"job_id": source_job.job_id},
            },
        },
        queue=ClioCoreQueue(tmp_path / "core"),
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        profile="user",
    )

    assert response is not None and "error" not in response, response
    tool_result = cast(dict[str, Any], response["result"])
    assert tool_result["isError"] is True
    public_receipt = cast(dict[str, Any], tool_result["structuredContent"])
    assert public_receipt["state"] == "succeeded"
    assert public_receipt["last_error"] is None
    assert public_receipt["mcp_result_artifact"] == {
        "artifact_id": artifact.artifact_id,
        "job_id": source_job.job_id,
        "kind": "mcp_result",
        "size_bytes": len(payload),
        "sha256": artifact.sha256,
        "created_at": artifact.model_dump(mode="json")["created_at"],
    }
    delivery = cast(dict[str, Any], public_receipt["mcp_result"])["delivery"]
    assert delivery["status"] == "failed"
    assert delivery["private_evidence_preserved"] is True
    assert delivery["remote_side_effects_may_have_occurred"] is True
    serialized = json.dumps(response, sort_keys=True)
    assert "Remote side effects may have occurred" in serialized
    assert secret not in serialized
    assert parsed.document["structured_result"] == {"application_payload": secret}


def test_owned_registered_remote_mcp_call_uses_authenticated_session_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        namespace="science",
        allow_tools=["inspect"],
        profiles=["user"],
    )
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares-login",
        remote_mcp_servers={"science": registration},
    )
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "session-api-token")
    monkeypatch.setenv("CLIO_RELAY_OWNER_SESSION_ID", "desktop-session-1")
    monkeypatch.setenv("CLIO_RELAY_SESSION_GENERATION_ID", "generation-1")
    monkeypatch.setenv("CLIO_RELAY_OWNER_SESSION_CLUSTER", "ares")
    monkeypatch.delenv("CLIO_RELAY_REMOTE_CLUSTER", raising=False)
    monkeypatch.delenv("CLIO_RELAY_CLI_MODE", raising=False)
    route = RemoteMcpRoute(
        cluster="ares",
        server_name="science",
        command="science-mcp",
        args=("--stdio",),
        env_from=(),
        expected_server_artifact_digest="c" * 64,
        remote_tool_name="inspect",
        timeout_seconds=300,
        contract=None,
        cluster_route_revision=cluster_route_revision(definition),
        registration_revision=remote_mcp_registration_revision(registration),
        control_query_evidence=McpControlQueryEvidence(
            cluster="ares",
            registered_server_name="science",
            cluster_route_revision=cluster_route_revision(definition),
            registration_revision=remote_mcp_registration_revision(registration),
            discovery_job_id="job_discovery",
            discovery_artifact_id="artifact_result",
            discovery_artifact_sha256="d" * 64,
            discovery_schema_digest="e" * 64,
            expected_server_artifact_digest="c" * 64,
        ),
    )
    virtual_tool = VirtualRemoteMcpTool(
        alias="science_inspect",
        namespace="science",
        remote_tool=RemoteMcpToolSchema(
            name="inspect",
            input_schema={
                "type": "object",
                "properties": {"dataset": {"type": "string"}},
                "required": ["dataset"],
                "additionalProperties": False,
            },
            annotations={"readOnlyHint": True, "destructiveHint": False},
        ),
        routes={"ares": route},
        arguments_wrapped=False,
    )
    catalog = VirtualRemoteMcpCatalog(
        revision="catalog-revision-1",
        tools={"science_inspect": virtual_tool},
        issues=(),
        cluster_route_revisions={"ares": cluster_route_revision(definition)},
    )

    def selected_catalog(*, profile: str, reserved_names: set[str]) -> VirtualRemoteMcpCatalog:
        del profile, reserved_names
        return catalog

    monkeypatch.setattr(mcp_server_module, "_remote_mcp_catalog", selected_catalog)
    captured: dict[str, object] = {}
    submitted_jobs: list[RelayJob] = []

    def submit_owned(**kwargs: object) -> RelayJob:
        captured.update(kwargs)
        payload = cast(dict[str, object], kwargs["payload"])
        selected_settings = cast(RelaySettings, kwargs["settings"])
        evidence = McpControlQueryEvidence.model_validate(payload["control_query_evidence"])
        authority = McpAdmissionAuthority(
            source="registered_discovery_artifact",
            operation=McpOperation.TOOLS_CALL,
            tool=cast(str, payload["tool"]),
            expected_server_artifact_digest=cast(
                str,
                payload["expected_server_artifact_digest"],
            ),
            evidence=evidence,
        )
        job = RelayJob(
            cluster="ares",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=cast(str, payload["server"]),
                server_args=cast(list[str], payload["server_args"]),
                expected_server_artifact_digest=cast(
                    str,
                    payload["expected_server_artifact_digest"],
                ),
                admission_class=McpAdmissionClass.CONTROL_QUERY,
                tool=cast(str, payload["tool"]),
                arguments=cast(dict[str, object], payload["arguments"]),
            ),
            idempotency_key=cast(str, payload["idempotency_key"]),
            metadata={
                "owner": "clio-relay",
                "owner_session_id": selected_settings.owner_session_id,
                "owner_session_generation_id": (selected_settings.owner_session_generation_id),
                MCP_ADMISSION_AUTHORITY_METADATA_KEY: authority.model_dump(mode="json"),
            },
        )
        submitted_jobs.append(job)
        return job

    monkeypatch.setattr("clio_relay.mcp_server.submit_owned_session_job", submit_owned)
    settings = RelaySettings.from_env()
    assert settings.owner_session_cluster == "ares"
    assert settings.remote_cluster is None
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    assert (
        handle_request(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            queue=queue,
            settings=settings,
            profile="user",
            session=session,
        )
        is not None
    )

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "science_inspect",
                "arguments": {"cluster": "ares", "dataset": "asteroid2018"},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )

    assert response is not None
    assert "result" in response, response
    assert captured["path"] == "/jobs/mcp-call"
    payload = cast(dict[str, object], captured["payload"])
    assert payload["server"] == "science-mcp"
    assert payload["expected_server_artifact_digest"] == "c" * 64
    assert "admission_class" not in payload
    assert McpControlQueryEvidence.model_validate(payload["control_query_evidence"]) == (
        route.control_query_evidence
    )
    assert payload["arguments"] == {"dataset": "asteroid2018"}
    assert queue.list_jobs() == []

    submitted = submitted_jobs[0]
    assert isinstance(submitted.spec, McpCallSpec)
    assert submitted.spec.admission_class is McpAdmissionClass.CONTROL_QUERY
    terminal = submitted.model_copy(update={"state": JobState.SUCCEEDED})
    requests: list[tuple[str, str]] = []

    class FakeOwnedSessionApiClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> FakeOwnedSessionApiClient:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def request_json(
            self,
            *,
            method: str,
            path: str,
            query: dict[str, object] | None = None,
            body: dict[str, object] | None = None,
            response_timeout_seconds: float | None = None,
        ) -> object:
            del query, body, response_timeout_seconds
            requests.append((method, path))
            if path == f"/jobs/{submitted.job_id}/status":
                return {
                    "job": terminal.model_dump(mode="json"),
                    "relay_queue": {},
                    "scheduler": [],
                    "terminal": True,
                }
            if path == f"/jobs/{submitted.job_id}/monitor":
                return {
                    "job": submitted.model_dump(mode="json"),
                    "relay_queue": {},
                    "scheduler": [],
                    "terminal": False,
                    "events": [],
                    "next_cursor": 1,
                }
            if path == f"/jobs/{submitted.job_id}/wait":
                return terminal.model_dump(mode="json")
            if path == f"/queue/jobs/{submitted.job_id}/cancel":
                return {
                    "job": terminal.model_dump(mode="json"),
                    "scheduler_policy": "relay-only",
                }
            if path == f"/jobs/{submitted.job_id}/artifacts":
                return {
                    "artifacts": [],
                    "cursor": 1,
                    "limit": 500,
                    "next_cursor": None,
                    "total": 0,
                }
            raise AssertionError(f"unexpected owned session request: {method} {path}")

    monkeypatch.setattr(mcp_server_module, "OwnedSessionApiClient", FakeOwnedSessionApiClient)
    followups: list[tuple[str, dict[str, object]]] = [
        ("relay_status", {}),
        ("relay_cancel", {}),
        ("relay_observe", {"include_logs": False}),
        ("relay_wait", {"include_logs": False, "timeout_seconds": 1}),
    ]
    followup_responses = [
        handle_request(
            {
                "jsonrpc": "2.0",
                "id": index,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": {"job_id": submitted.job_id, **controls},
                },
            },
            queue=queue,
            settings=settings,
            profile="user",
            session=session,
        )
        for index, (tool_name, controls) in enumerate(followups, start=3)
    ]

    assert all(item is not None and "error" not in item for item in followup_responses)
    assert requests == [
        ("GET", f"/jobs/{submitted.job_id}/status"),
        ("POST", f"/queue/jobs/{submitted.job_id}/cancel"),
        ("GET", f"/jobs/{submitted.job_id}/monitor"),
        ("POST", f"/jobs/{submitted.job_id}/wait"),
        ("GET", f"/jobs/{submitted.job_id}/status"),
        ("GET", f"/jobs/{submitted.job_id}/artifacts"),
    ]

    ClusterRegistry(
        clusters={"ares": ClusterDefinition(name="ares", ssh_host="replacement-login")}
    ).save(registry_path)
    stale = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "relay_status",
                "arguments": {"job_id": submitted.job_id},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )
    assert stale is not None
    assert "cluster route changed" in stale["error"]["message"]
    assert len(requests) == 6


def test_registered_remote_mcp_ssh_forwarding_carries_evidence_not_lane(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SSH fallback lets the cluster receiver derive admission from exact evidence."""
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        allow_tools=["inspect"],
    )
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares-login",
        remote_mcp_servers={"science": registration},
    )
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    evidence = McpControlQueryEvidence(
        cluster="ares",
        registered_server_name="science",
        cluster_route_revision=cluster_route_revision(definition),
        registration_revision=remote_mcp_registration_revision(registration),
        discovery_job_id="job_discovery",
        discovery_artifact_id="artifact_result",
        discovery_artifact_sha256="d" * 64,
        discovery_schema_digest="e" * 64,
        expected_server_artifact_digest="c" * 64,
    )
    commands: list[list[str]] = []

    def ignore_write(_definition: ClusterDefinition, _path: str, _data: bytes) -> None:
        return None

    def ignore_remove(
        _definition: ClusterDefinition,
        _path: str,
        *,
        remove_empty_parent: bool,
    ) -> None:
        del remove_empty_parent

    monkeypatch.setattr(mcp_server_module, "write_remote_file", ignore_write)
    monkeypatch.setattr(mcp_server_module, "remove_remote_file", ignore_remove)

    def run_remote(_definition: ClusterDefinition, command: list[str]) -> str:
        commands.append(command)
        return "job_remote_registered\n"

    monkeypatch.setattr(mcp_server_module, "run_remote_clio", run_remote)
    result = mcp_server_module._submit_mcp_call(  # pyright: ignore[reportPrivateUsage]
        {
            "cluster": "ares",
            "server": registration.command,
            "server_args": registration.args,
            "tool": "inspect",
            "arguments": {"dataset": "asteroid2018"},
            "timeout_seconds": registration.call_timeout_seconds,
            "expected_server_artifact_digest": "c" * 64,
            "registered_route": True,
            "registered_remote_mcp_route": True,
            "expected_cluster_route_revision": cluster_route_revision(definition),
            "registered_server_name": "science",
            "expected_remote_mcp_registration_revision": (
                remote_mcp_registration_revision(registration)
            ),
            "control_query_evidence": evidence.model_dump(mode="json"),
        },
        queue=ClioCoreQueue(tmp_path / "core"),
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
    )

    assert result["job_id"] == "job_remote_registered"
    command = commands[0]
    assert "--admission-class" not in command
    evidence_json = command[command.index("--control-query-evidence-json") + 1]
    assert McpControlQueryEvidence.model_validate_json(evidence_json) == evidence


def test_virtual_remote_mcp_idempotency_replays_exactly_and_conflicts_on_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expose a relay-only retry key without leaking it into remote tool arguments."""
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        allow_tools=["mutate"],
        profiles=["user"],
    )
    definition = ClusterDefinition(
        name="alpha",
        ssh_host="localhost",
        remote_mcp_servers={"science": registration},
    )
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"alpha": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    route = RemoteMcpRoute(
        cluster="alpha",
        server_name="science",
        command=registration.command,
        args=tuple(registration.args),
        env_from=(),
        expected_server_artifact_digest=None,
        remote_tool_name="mutate",
        timeout_seconds=registration.call_timeout_seconds,
        contract=None,
        cluster_route_revision=cluster_route_revision(definition),
        registration_revision=remote_mcp_registration_revision(registration),
    )
    catalog = VirtualRemoteMcpCatalog(
        revision="f" * 64,
        tools={
            "science_mutate": VirtualRemoteMcpTool(
                alias="science_mutate",
                namespace="science",
                remote_tool=RemoteMcpToolSchema(
                    name="mutate",
                    input_schema={
                        "type": "object",
                        "properties": {"value": {"type": "integer"}},
                        "required": ["value"],
                        "additionalProperties": False,
                    },
                    annotations={"readOnlyHint": False, "destructiveHint": True},
                ),
                routes={"alpha": route},
                arguments_wrapped=False,
            )
        },
        issues=(),
        cluster_route_revisions={"alpha": cluster_route_revision(definition)},
    )

    def selected_catalog(*, profile: str, reserved_names: set[str]) -> VirtualRemoteMcpCatalog:
        del profile, reserved_names
        return catalog

    monkeypatch.setattr(mcp_server_module, "_remote_mcp_catalog", selected_catalog)
    queue = ClioCoreQueue(tmp_path / "core")
    session = McpSessionState()
    assert (
        handle_request(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            queue=queue,
            profile="user",
            session=session,
        )
        is not None
    )

    def invoke(request_id: int, value: int) -> dict[str, Any] | None:
        return handle_request(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "tools/call",
                "params": {
                    "name": "science_mutate",
                    "arguments": {
                        "cluster": "alpha",
                        "value": value,
                        "idempotency_key": "agent-retry-1",
                    },
                },
            },
            queue=queue,
            profile="user",
            session=session,
        )

    first = invoke(2, 1)
    replay = invoke(3, 1)
    conflict = invoke(4, 2)

    assert first is not None and replay is not None and conflict is not None
    first_job_id = first["result"]["structuredContent"]["job_id"]
    assert replay["result"]["structuredContent"]["job_id"] == first_job_id
    assert "idempotency" in conflict["error"]["message"].lower()
    assert len(queue.list_jobs()) == 1
    job = queue.get_job(first_job_id)
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.arguments == {"value": 1}
    assert job.idempotency_key == "agent-retry-1"


def test_virtual_remote_mcp_wait_envelope_returns_same_call_result_without_leaking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reproduce the live catalog failure and prove relay controls stay local."""

    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        namespace="science",
        allow_tools=["scientific_dataset_search"],
        profiles=["user"],
    )
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares-login",
        remote_mcp_servers={"science": registration},
    )
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    route = RemoteMcpRoute(
        cluster="ares",
        server_name="science",
        command="science-mcp",
        args=("--stdio",),
        env_from=(),
        expected_server_artifact_digest="c" * 64,
        remote_tool_name="scientific_dataset_search",
        timeout_seconds=300,
        contract=None,
        cluster_route_revision=cluster_route_revision(definition),
        registration_revision=remote_mcp_registration_revision(registration),
    )
    remote_schema = {
        "type": "object",
        "properties": {
            "query": {"type": "string"},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
        },
        "required": ["query"],
        "additionalProperties": False,
    }
    virtual_tool = VirtualRemoteMcpTool(
        alias="science_scientific_dataset_search",
        namespace="science",
        remote_tool=RemoteMcpToolSchema(
            name="scientific_dataset_search",
            input_schema=remote_schema,
        ),
        routes={"ares": route},
        arguments_wrapped=False,
    )
    catalog = VirtualRemoteMcpCatalog(
        revision="d" * 64,
        tools={virtual_tool.alias: virtual_tool},
        issues=(),
        cluster_route_revisions={"ares": cluster_route_revision(definition)},
    )

    def selected_catalog(*, profile: str, reserved_names: set[str]) -> VirtualRemoteMcpCatalog:
        del profile, reserved_names
        return catalog

    monkeypatch.setattr(mcp_server_module, "_remote_mcp_catalog", selected_catalog)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    wait_observation: dict[str, object] = {}

    def complete_wait(
        selected_queue: ClioCoreQueue,
        job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> RelayJob:
        wait_observation.update(
            {
                "queue": selected_queue,
                "job_id": job_id,
                "timeout_seconds": timeout_seconds,
                "poll_seconds": poll_seconds,
            }
        )
        return selected_queue.get_job(job_id).model_copy(update={"state": JobState.SUCCEEDED})

    def artifacts(_queue: ClioCoreQueue, job_id: str) -> list[dict[str, object]]:
        return [
            {
                "artifact_id": "artifact_virtual_search_result",
                "job_id": job_id,
                "kind": "mcp_result",
                "size_bytes": 128,
                "sha256": "e" * 64,
                "created_at": "2026-07-16T15:00:00Z",
            }
        ]

    def verified_result(
        _queue: ClioCoreQueue,
        _job_id: str,
    ) -> mcp_server_module._VerifiedMcpResult:  # pyright: ignore[reportPrivateUsage]
        document: dict[str, Any] = {
            "operation": "tools/call",
            "tool": "scientific_dataset_search",
            "returncode": 0,
            "structured_result": {"datasets": [{"dataset_id": "asteroid-first-five"}]},
        }
        return mcp_server_module._VerifiedMcpResult(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            document=document,
            public=document,
        )

    def bounded_logs(
        selected_queue: ClioCoreQueue,
        selected_settings: RelaySettings,
        job_id: str,
        *,
        limit: int,
    ) -> dict[str, object]:
        assert selected_queue is queue
        assert selected_settings is settings
        assert limit == 1_024
        return {
            "stdout": {"job_id": job_id, "text": "catalog complete", "eof": True},
            "stderr": {"job_id": job_id, "text": "", "eof": True},
        }

    monkeypatch.setattr(mcp_server_module, "wait_for_terminal", complete_wait)
    monkeypatch.setattr(mcp_server_module, "_complete_local_artifacts", artifacts)
    monkeypatch.setattr(mcp_server_module, "_verified_local_mcp_result", verified_result)
    monkeypatch.setattr(mcp_server_module, "_job_logs", bounded_logs)
    session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )
    assert listed is not None
    advertised = next(
        tool
        for tool in listed["result"]["tools"]
        if tool["name"] == "science_scientific_dataset_search"
    )
    invocation = {
        "cluster": "ares",
        "query": "2018 asteroid impact",
        "page_size": 20,
        "wait_for_terminal": True,
        "wait_timeout_seconds": 45,
        "poll_seconds": 0.25,
        "include_logs": True,
        "log_limit": 1_024,
    }
    cast(_SchemaValidator, Draft202012Validator(advertised["inputSchema"])).validate(invocation)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": virtual_tool.alias, "arguments": invocation},
        },
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )

    assert response is not None and "error" not in response, response
    result = response["result"]["structuredContent"]
    cast(_SchemaValidator, Draft202012Validator(advertised["outputSchema"])).validate(result)
    assert result["terminal"] is True
    assert result["state"] == "succeeded"
    assert result["mcp_result"]["structured_result"]["datasets"][0]["dataset_id"] == (
        "asteroid-first-five"
    )
    assert result["logs"]["stdout"]["text"] == "catalog complete"
    submitted = queue.list_jobs()[0]
    assert isinstance(submitted.spec, McpCallSpec)
    assert submitted.spec.arguments == {
        "query": "2018 asteroid impact",
        "page_size": 20,
    }
    assert wait_observation == {
        "queue": queue,
        "job_id": submitted.job_id,
        "timeout_seconds": 45.0,
        "poll_seconds": 0.25,
    }


def test_owned_remote_agent_submission_uses_authenticated_session_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    captured: dict[str, object] = {}

    def submit_owned(**kwargs: object) -> RelayJob:
        captured.update(kwargs)
        payload = cast(dict[str, object], kwargs["payload"])
        selected_settings = cast(RelaySettings, kwargs["settings"])
        return RelayJob(
            cluster="ares",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(
                prompt_path=cast(str, payload["prompt_path"]),
                workdir=cast(str, payload["workdir"]),
                timeout_seconds=cast(int, payload["timeout_seconds"]),
            ),
            idempotency_key=cast(str, payload["idempotency_key"]),
            metadata={
                "owner": "clio-relay",
                "owner_session_id": selected_settings.owner_session_id,
                "owner_session_generation_id": (selected_settings.owner_session_generation_id),
            },
        )

    monkeypatch.setattr("clio_relay.mcp_server.submit_owned_session_job", submit_owned)

    def fail_remote(_definition: ClusterDefinition, _args: list[str]) -> str:
        raise AssertionError("owned agent submission bypassed the session API")

    monkeypatch.setattr("clio_relay.mcp_server.run_remote_clio", fail_remote)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
    )
    queue = ClioCoreQueue(settings.core_dir)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_agent",
                "arguments": {
                    "cluster": "ares",
                    "prompt_path": "/remote/demo/prompt.md",
                    "workdir": "/remote/demo",
                    "timeout_seconds": 120,
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )

    assert response is not None
    assert "result" in response, response
    assert captured["path"] == "/jobs/remote-agent"
    payload = cast(dict[str, object], captured["payload"])
    assert payload["prompt_path"] == "/remote/demo/prompt.md"
    assert payload["workdir"] == "/remote/demo"
    assert payload["timeout_seconds"] == 120
    assert queue.list_jobs() == []


def test_owned_remote_followups_use_session_api_and_never_direct_ssh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    running = RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_name="pipeline"),
        idempotency_key="owned-followup",
        metadata={
            "owner": "clio-relay",
            "owner_session_id": "desktop-session-1",
            "owner_session_generation_id": "generation-1",
        },
    )
    terminal = running.model_copy(update={"state": JobState.SUCCEEDED})
    requests: list[dict[str, object]] = []
    client_instances: list[int] = []

    class FakeOwnedSessionApiClient:
        def __init__(self, **_kwargs: object) -> None:
            self.instance = len(client_instances) + 1
            client_instances.append(self.instance)

        def __enter__(self) -> FakeOwnedSessionApiClient:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def request_json(
            self,
            *,
            method: str,
            path: str,
            query: dict[str, object] | None = None,
            body: dict[str, object] | None = None,
            response_timeout_seconds: float | None = None,
        ) -> object:
            requests.append(
                {
                    "instance": self.instance,
                    "method": method,
                    "path": path,
                    "query": query,
                    "body": body,
                    "response_timeout_seconds": response_timeout_seconds,
                }
            )
            if path == f"/jobs/{running.job_id}/status":
                return {
                    "job": terminal.model_dump(mode="json"),
                    "relay_queue": {},
                    "scheduler": [],
                    "terminal": True,
                }
            if path == f"/queue/jobs/{running.job_id}/cancel":
                return {
                    "job": terminal.model_dump(mode="json"),
                    "scheduler_policy": "relay-only",
                }
            if path == f"/jobs/{running.job_id}/monitor":
                return {
                    "job": running.model_dump(mode="json"),
                    "relay_queue": {},
                    "scheduler": [],
                    "terminal": False,
                    "events": [],
                    "next_cursor": 1,
                }
            if path == "/queue":
                return {"jobs": [], "count": 0, "visibility_filter": "exact_owner"}
            if path == f"/queue/jobs/{running.job_id}/diagnose":
                return {"job": running.model_dump(mode="json"), "reason": "running"}
            if path == f"/jobs/{running.job_id}/wait":
                return terminal.model_dump(mode="json")
            if path == f"/jobs/{running.job_id}/artifacts":
                return {
                    "artifacts": [],
                    "cursor": 1,
                    "limit": 500,
                    "next_cursor": None,
                    "total": 0,
                }
            raise AssertionError(f"unexpected owned session request: {method} {path}")

    def direct_ssh_forbidden(_definition: ClusterDefinition, _args: list[str]) -> str:
        raise AssertionError("owned follow-up bypassed the session API")

    monkeypatch.setattr(mcp_server_module, "OwnedSessionApiClient", FakeOwnedSessionApiClient)
    monkeypatch.setattr(mcp_server_module, "run_remote_clio", direct_ssh_forbidden)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="ares",
    )
    queue = ClioCoreQueue(settings.core_dir)
    route = {
        "cluster": "ares",
        "job_id": running.job_id,
        "route_revision": cluster_route_revision(definition),
    }
    calls = [
        ("relay_status", route),
        ("relay_cancel", route),
        ("relay_observe", {**route, "include_logs": False}),
        ("relay_queue_list", {"cluster": "ares", "limit": 10, "scan_limit": 20}),
        ("relay_queue_diagnose", route),
        ("relay_wait", {**route, "include_logs": False}),
    ]

    responses = [
        handle_request(
            {
                "jsonrpc": "2.0",
                "id": index,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            },
            queue=queue,
            settings=settings,
            profile="user",
        )
        for index, (name, arguments) in enumerate(calls, start=1)
    ]
    stale = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": {
                "name": "relay_queue_stale",
                "arguments": {"cluster": "ares", "older_than_seconds": 60},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )

    assert all(response is not None and "error" not in response for response in responses)
    assert stale is not None and "error" in stale
    assert "global queue visibility" in stale["error"]["message"]
    cancel_request = next(item for item in requests if cast(str, item["path"]).endswith("/cancel"))
    assert cancel_request["body"] == {
        "cluster": "ares",
        "cancel_scheduler_job": False,
    }
    assert not any("/logs/" in cast(str, item["path"]) for item in requests)
    wait_paths = [item["path"] for item in requests if item["instance"] == max(client_instances)]
    assert wait_paths == [
        f"/jobs/{running.job_id}/status",
        f"/jobs/{running.job_id}/artifacts",
    ]
    wait_request = next(item for item in requests if item["path"] == f"/jobs/{running.job_id}/wait")
    assert wait_request["response_timeout_seconds"] == 610
    assert wait_request["instance"] != max(client_instances)


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
    monkeypatch.setenv("JARVIS_MCP_SPACK_COMMAND", "/opt/site/spack/bin/spack")
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
    assert job.spec.env_from == {"JARVIS_MCP_SPACK_COMMAND": "JARVIS_MCP_SPACK_COMMAND"}
    execution_id = deterministic_jarvis_execution_id(
        cluster=job.cluster,
        idempotency_key=job.idempotency_key,
        job_id=job.job_id,
    )
    assert job.spec.arguments == {
        "pipeline_id": "example",
        "spack_specs": ["lammps@2024.08.29"],
        "execution_id": execution_id,
    }


def test_mcp_virtual_jarvis_execution_query_routes_selectors_through_remote_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_cluster(tmp_path, monkeypatch, "test-cluster")
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 261,
            "method": "tools/call",
            "params": {
                "name": "jarvis_get_execution",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_id": "example",
                    "execution_id": "jarvis_execution_1",
                    "include_progress": False,
                    "artifacts": {
                        "package_id": "gray-scott",
                        "role": "output",
                        "state": "finalized",
                        "artifact_id": "art_0000000000000000000001",
                        "page_size": 25,
                        "cursor": "opaque_cursor_1",
                    },
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    job = queue.get_job(response["result"]["structuredContent"]["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.tool == "jarvis_get_execution"
    assert job.spec.admission_class is McpAdmissionClass.CONTROL_QUERY
    assert job.spec.timeout_seconds == 60
    assert job.spec.arguments == {
        "pipeline_id": "example",
        "execution_id": "jarvis_execution_1",
        "include_progress": False,
        "artifacts": {
            "package_id": "gray-scott",
            "role": "output",
            "state": "finalized",
            "artifact_id": "art_0000000000000000000001",
            "page_size": 25,
            "cursor": "opaque_cursor_1",
        },
    }


def test_jarvis_default_key_changes_after_artifact_bound_control_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An earlier unbound workload call cannot alias a later pinned control query."""
    _configure_local_cluster(tmp_path, monkeypatch, "test-cluster")
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    arguments = {
        "cluster": "test-cluster",
        "tool": "jarvis_get_execution",
        "arguments": {"pipeline_id": "example", "execution_id": "execution-1"},
    }

    unbound = mcp_server_module._submit_jarvis_mcp_call(  # pyright: ignore[reportPrivateUsage]
        arguments,
        queue=queue,
        settings=settings,
    )
    bound = mcp_server_module._submit_jarvis_mcp_call(  # pyright: ignore[reportPrivateUsage]
        {**arguments, "registered_route": True},
        queue=queue,
        settings=settings,
    )
    unbound_job = queue.get_job(cast(str, unbound["job_id"]))
    bound_job = queue.get_job(cast(str, bound["job_id"]))

    assert unbound_job.job_id != bound_job.job_id
    assert unbound_job.idempotency_key != bound_job.idempotency_key
    legacy_digest = hashlib.sha256(
        json.dumps(
            arguments["arguments"],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert unbound_job.idempotency_key == (
        f"mcp:test-cluster:jarvis:jarvis_get_execution:{legacy_digest}"
    )
    assert isinstance(unbound_job.spec, McpCallSpec)
    assert isinstance(bound_job.spec, McpCallSpec)
    assert unbound_job.spec.expected_server_artifact_digest is None
    assert unbound_job.spec.admission_class is McpAdmissionClass.WORKLOAD
    assert unbound_job.spec.timeout_seconds is None
    assert bound_job.spec.expected_server_artifact_digest == "a" * 64
    assert bound_job.spec.admission_class is McpAdmissionClass.CONTROL_QUERY
    assert bound_job.spec.timeout_seconds == 60


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
    protocol_path = spool / "mcp-result.json"
    bearer = "a" * 64
    protocol_path.write_text(
        json.dumps({"authorization": {"scheme": "bearer", "token": bearer}}),
        encoding="utf-8",
    )
    internal_artifact = queue.append_artifact(
        ArtifactRef(job_id=job.job_id, uri=protocol_path.as_uri(), kind="mcp_result")
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
    internal_content_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "relay_read_artifact",
                "arguments": {"artifact_id": internal_artifact.artifact_id},
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
    assert internal_content_response is not None
    assert internal_content_response["error"]["code"] == -32000
    serialized_error = json.dumps(internal_content_response, sort_keys=True)
    assert "not model-readable" in serialized_error
    assert bearer not in serialized_error


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


def test_mcp_queue_management_routes_configured_remote_cluster(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every queue operation must inspect the configured cluster queue, not desktop state."""
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(
        clusters={"cluster-a": ClusterDefinition(name="cluster-a", ssh_host="cluster-login")}
    ).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "auto")
    queue = ClioCoreQueue(tmp_path / "desktop-core")
    commands: list[list[str]] = []

    def run_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        commands.append(args)
        if args[:2] == ["queue", "list"]:
            return json.dumps({"jobs": [], "count": 0})
        if args[:2] == ["queue", "diagnose"]:
            return json.dumps({"job": {"job_id": "remote-job"}, "reason": "scheduler_pending"})
        if args[:2] == ["queue", "stale"]:
            return json.dumps({"jobs": [], "count": 0})
        if args[:2] == ["queue", "cleanup-stale"]:
            return json.dumps({"dry_run": False, "planned": [], "canceled_count": 0})
        if args[:2] == ["queue", "cancel"]:
            return json.dumps(
                {
                    "job": {"job_id": "remote-job", "state": "running"},
                    "scheduler_policy": "request-scheduler",
                }
            )
        if args[:2] == ["worker", "status"]:
            return json.dumps({"worker_count": 2, "configured_concurrency": 4})
        raise AssertionError(f"unexpected remote command: {args}")

    monkeypatch.setattr("clio_relay.mcp_server.run_remote_clio", run_remote)
    calls = [
        (
            "relay_queue_list",
            {
                "cluster": "cluster-a",
                "state": "queued",
                "kind": "remote_agent",
                "include_terminal": True,
                "cursor": 2,
                "limit": 10,
                "scan_limit": 20,
            },
        ),
        (
            "relay_queue_diagnose",
            {
                "cluster": "cluster-a",
                "job_id": "remote-job",
                "older_than_seconds": 60,
                "scan_limit": 25,
            },
        ),
        (
            "relay_queue_stale",
            {
                "cluster": "cluster-a",
                "job_id": "remote-job",
                "older_than_seconds": 120,
                "kind": "mcp_call",
                "limit": 5,
                "scan_limit": 20,
            },
        ),
        (
            "relay_queue_cleanup_stale",
            {
                "cluster": "cluster-a",
                "job_id": "remote-job",
                "older_than_seconds": 180,
                "kind": "jarvis",
                "max_attempts": 5,
                "dry_run": False,
                "cancel_queued": True,
                "limit": 6,
                "scan_limit": 20,
            },
        ),
        (
            "relay_cancel_job",
            {
                "cluster": "cluster-a",
                "job_id": "remote-job",
                "cancel_scheduler_job": True,
            },
        ),
        ("relay_worker_status", {"cluster": "cluster-a"}),
    ]

    responses = [
        handle_request(
            {
                "jsonrpc": "2.0",
                "id": index,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            },
            queue=queue,
            profile=(
                "user"
                if name
                in {
                    "relay_queue_list",
                    "relay_queue_diagnose",
                    "relay_queue_stale",
                }
                else "admin"
            ),
        )
        for index, (name, arguments) in enumerate(calls, start=1)
    ]

    assert all(response is not None and "error" not in response for response in responses)
    route_revisions = {
        response["result"]["structuredContent"]["route_revision"]
        for response in responses
        if response is not None
    }
    assert len(route_revisions) == 1
    route_revision = next(iter(route_revisions))
    assert all(
        response is not None
        and response["result"]["structuredContent"]["cluster"] == "cluster-a"
        and response["result"]["structuredContent"]["remote"] is True
        for response in responses
    )
    assert commands == [
        [
            "queue",
            "list",
            "--cluster",
            "cluster-a",
            "--cursor",
            "2",
            "--limit",
            "10",
            "--scan-limit",
            "20",
            "--state",
            "queued",
            "--kind",
            "remote_agent",
            "--include-terminal",
        ],
        [
            "queue",
            "diagnose",
            "remote-job",
            "--cluster",
            "cluster-a",
            "--older-than",
            "60s",
            "--scan-limit",
            "25",
        ],
        [
            "queue",
            "stale",
            "--cluster",
            "cluster-a",
            "--older-than",
            "120s",
            "--limit",
            "5",
            "--scan-limit",
            "20",
            "--job-id",
            "remote-job",
            "--kind",
            "mcp_call",
        ],
        [
            "queue",
            "cleanup-stale",
            "--cluster",
            "cluster-a",
            "--older-than",
            "180s",
            "--max-attempts",
            "5",
            "--limit",
            "6",
            "--scan-limit",
            "20",
            "--no-dry-run",
            "--job-id",
            "remote-job",
            "--kind",
            "jarvis",
            "--cancel-queued",
        ],
        [
            "queue",
            "cancel",
            "remote-job",
            "--cluster",
            "cluster-a",
            "--cancel-scheduler-job",
        ],
        ["worker", "status", "--cluster", "cluster-a"],
    ]
    assert queue.list_jobs() == []

    ClusterRegistry(
        clusters={"cluster-a": ClusterDefinition(name="cluster-a", ssh_host="replacement-login")}
    ).save(registry_path)
    command_count = len(commands)
    stale_route = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 100,
            "method": "tools/call",
            "params": {
                "name": "relay_queue_diagnose",
                "arguments": {
                    "cluster": "cluster-a",
                    "route_revision": route_revision,
                    "job_id": "remote-job",
                },
            },
        },
        queue=queue,
        profile="admin",
    )
    assert stale_route is not None
    assert "cluster route changed" in stale_route["error"]["message"]
    assert len(commands) == command_count


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

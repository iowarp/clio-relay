from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

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
    JarvisRunSpec,
    JobKind,
    McpCallSpec,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
)


def test_mcp_lists_relay_tools(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
    )

    assert response is not None
    tool_names = {tool["name"] for tool in response["result"]["tools"]}
    assert "relay_submit_jarvis_pipeline" in tool_names
    assert "relay_submit_remote_agent" in tool_names
    assert "relay_submit_mcp_call" in tool_names
    assert "relay_get_job" in tool_names
    assert "relay_monitor_job" in tool_names
    assert "relay_watch_job_events" in tool_names
    assert "relay_list_tasks" in tool_names
    assert "relay_read_job_log" in tool_names
    assert "relay_list_artifacts" in tool_names
    assert "relay_read_artifact" in tool_names
    assert "relay_record_progress" in tool_names
    assert "relay_list_progress" in tool_names
    assert "relay_cancel_job" in tool_names
    assert "relay_create_monitor_rule" in tool_names
    assert "relay_list_monitor_rules" in tool_names
    assert "relay_evaluate_monitor_rules" in tool_names


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
                    "tool": "run",
                    "arguments": {"case": "lammps", "steps": 100},
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
    assert job.spec.tool == "run"
    assert job.spec.arguments == {"case": "lammps", "steps": 100}
    assert job.spec.timeout_seconds == 60


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
        "arguments": {"case": "lammps"},
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
                        "adapter": "lammps",
                        "package_name": "builtin.lammps",
                        "package_version": "builtin",
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
    assert response["result"]["structuredContent"]["state"] == "canceled"
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)
    assert [event.event_type for event in events][-2:] == [
        "job.cancel_requested",
        "job.canceled",
    ]


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
    )

    response = json.loads(stdout.getvalue())
    tool_names = {tool["name"] for tool in response["result"]["tools"]}
    assert "relay_submit_remote_agent" in tool_names

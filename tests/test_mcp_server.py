from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.mcp_server import handle_request, render_codex_mcp_profile, serve_stdio
from clio_relay.models import JarvisRunSpec, JobKind


def test_mcp_lists_relay_tools(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
    )

    assert response is not None
    tool_names = {tool["name"] for tool in response["result"]["tools"]}
    assert "relay_submit_jarvis_pipeline" in tool_names
    assert "relay_get_job" in tool_names
    assert "relay_watch_job_events" in tool_names


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


def test_codex_mcp_profile_points_to_clio_relay_server() -> None:
    rendered = render_codex_mcp_profile(
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


def test_stdio_server_reports_parse_errors(tmp_path: Path) -> None:
    stdout = StringIO()

    serve_stdio(
        stdin=StringIO("not-json\n"),
        stdout=stdout,
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
    )

    response = json.loads(stdout.getvalue())
    assert response["error"]["code"] == -32700

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from fastapi.testclient import TestClient

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.http_api import create_app
from clio_relay.models import (
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    EndpointRole,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    MonitorRule,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    TaskTimelineEvent,
)


def test_http_monitor_logs_and_artifact_content(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    stdout_path = spool / "stdout.log"
    stdout_path.write_text("hello from http\n", encoding="utf-8")
    artifact = queue.append_artifact(
        ArtifactRef(job_id=job.job_id, uri=stdout_path.as_uri(), kind="stdout")
    )
    client = cast(Any, TestClient(create_app(settings)))

    monitor_response = client.get(f"/jobs/{job.job_id}/monitor")
    log_response = client.get(f"/jobs/{job.job_id}/logs/stdout", params={"limit": 5})
    artifact_response = client.get(f"/artifacts/{artifact.artifact_id}/content")

    assert monitor_response.status_code == 200
    assert monitor_response.json()["job"]["job_id"] == job.job_id
    assert log_response.status_code == 200
    assert log_response.json()["text"] == "hello"
    assert artifact_response.status_code == 200
    assert artifact_response.json()["artifact"]["artifact_id"] == artifact.artifact_id


def test_http_monitor_sse_streams_monitor_and_terminal_events(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-sse",
        )
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    client = cast(Any, TestClient(create_app(settings)))

    with client.stream("GET", f"/jobs/{job.job_id}/monitor/sse") as response:
        body = response.read().decode("utf-8")

    assert response.status_code == 200
    assert "event: monitor" in body
    assert "event: terminal" in body
    assert job.job_id in body


def test_http_monitor_websocket_streams_monitor_and_terminal_events(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-websocket",
        )
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    client = cast(Any, TestClient(create_app(settings)))

    with client.websocket_connect(f"/jobs/{job.job_id}/monitor/ws") as websocket:
        monitor = websocket.receive_json()
        terminal = websocket.receive_json()

    assert monitor["event"] == "monitor"
    assert monitor["data"]["job"]["job_id"] == job.job_id
    assert terminal == {
        "event": "terminal",
        "data": {"job_id": job.job_id, "state": "succeeded"},
    }


def test_http_monitor_websocket_streams_running_then_terminal(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-websocket-running",
        )
    )
    queue.update_job_state(job.job_id, JobState.RUNNING)
    client = cast(Any, TestClient(create_app(settings)))

    with client.websocket_connect(f"/jobs/{job.job_id}/monitor/ws?poll_seconds=0.01") as websocket:
        running = websocket.receive_json()
        queue.update_job_state(job.job_id, JobState.SUCCEEDED)
        messages: list[dict[str, Any]] = []
        for _ in range(10):
            message = cast(dict[str, Any], websocket.receive_json())
            messages.append(message)
            if message["event"] == "terminal":
                break

    assert running["event"] == "monitor"
    assert running["data"]["job"]["state"] == "running"
    assert {
        "event": "terminal",
        "data": {"job_id": job.job_id, "state": "succeeded"},
    } in messages


def test_http_api_enforces_configured_token(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="secret-token",
    )
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-auth",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    missing = client.get(f"/jobs/{job.job_id}")
    wrong = client.get(f"/jobs/{job.job_id}", headers={"Authorization": "Bearer wrong"})
    bearer = client.get(f"/jobs/{job.job_id}", headers={"Authorization": "Bearer secret-token"})
    explicit = client.get(f"/jobs/{job.job_id}", headers={"X-Clio-Relay-Token": "secret-token"})

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert bearer.status_code == 200
    assert explicit.status_code == 200


def test_http_typed_submit_endpoints_create_real_jobs(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    client = cast(Any, TestClient(create_app(settings)))
    prompt_path = tmp_path / "prompt.md"
    mcp_config_path = tmp_path / "mcp.toml"

    jarvis_response = client.post(
        "/jobs/jarvis",
        json={
            "cluster": "test-cluster",
            "pipeline_yaml": "name: generic\npkgs: []\n",
            "idempotency_key": "http-typed-jarvis",
        },
    )
    jarvis_pipeline_response = client.post(
        "/jobs/jarvis-pipeline",
        json={
            "cluster": "test-cluster",
            "pipeline_name": "lammps_4node",
            "idempotency_key": "http-typed-jarvis-pipeline",
        },
    )
    agent_response = client.post(
        "/jobs/remote-agent",
        json={
            "cluster": "test-cluster",
            "prompt_path": str(prompt_path),
            "mcp_config_path": str(mcp_config_path),
            "model": "configured-model",
            "workdir": str(tmp_path),
            "timeout_seconds": 60,
            "idempotency_key": "http-typed-agent",
        },
    )
    mcp_response = client.post(
        "/jobs/mcp-call",
        json={
            "cluster": "test-cluster",
            "server": "remote-server",
            "server_args": ["--stdio"],
            "tool": "simulate",
            "arguments": {"case": "lammps", "steps": 100},
            "timeout_seconds": 30,
            "idempotency_key": "http-typed-mcp",
        },
    )
    jarvis_mcp_response = client.post(
        "/jobs/jarvis-mcp-call",
        json={
            "cluster": "test-cluster",
            "tool": "jm_list_repos",
            "idempotency_key": "http-typed-jarvis-mcp",
        },
    )

    assert jarvis_response.status_code == 200
    assert jarvis_pipeline_response.status_code == 200
    assert agent_response.status_code == 200
    assert mcp_response.status_code == 200
    assert jarvis_mcp_response.status_code == 200
    jarvis = queue.get_job(jarvis_response.json()["job_id"])
    jarvis_pipeline = queue.get_job(jarvis_pipeline_response.json()["job_id"])
    agent = queue.get_job(agent_response.json()["job_id"])
    mcp = queue.get_job(mcp_response.json()["job_id"])
    jarvis_mcp = queue.get_job(jarvis_mcp_response.json()["job_id"])
    assert jarvis.kind == JobKind.JARVIS
    assert isinstance(jarvis.spec, JarvisRunSpec)
    assert isinstance(jarvis_pipeline.spec, JarvisRunSpec)
    assert jarvis_pipeline.spec.pipeline_name == "lammps_4node"
    assert agent.kind == JobKind.REMOTE_AGENT
    assert isinstance(agent.spec, RemoteAgentTaskSpec)
    assert agent.spec.prompt_path == str(prompt_path)
    assert agent.spec.mcp_config_path == str(mcp_config_path)
    assert agent.spec.model == "configured-model"
    assert mcp.kind == JobKind.MCP_CALL
    assert isinstance(mcp.spec, McpCallSpec)
    assert mcp.spec.server_args == ["--stdio"]
    assert mcp.spec.arguments == {"case": "lammps", "steps": 100}
    assert isinstance(jarvis_mcp.spec, McpCallSpec)
    assert jarvis_mcp.spec.server == "jarvis-mcp"
    assert jarvis_mcp.spec.server_args == ["--profile", "user"]
    assert jarvis_mcp.spec.tool == "jm_list_repos"


def test_http_progress_endpoints_record_and_list_progress(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-progress",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    record_response = client.post(
        f"/jobs/{job.job_id}/progress",
        json={
            "label": "iteration",
            "current": 5,
            "total": 10,
            "unit": "step",
            "message": "half way",
            "metadata": {
                "source": "jarvis_package",
                "adapter": "lammps",
                "package_name": "builtin.lammps",
                "package_version": "builtin",
                "run_id": "spoofed",
            },
        },
    )
    list_response = client.get(f"/jobs/{job.job_id}/progress")

    assert record_response.status_code == 200
    assert list_response.status_code == 200
    recorded = record_response.json()
    listed = list_response.json()
    assert recorded["label"] == "iteration"
    assert recorded["current"] == 5
    assert recorded["metadata"]["source"] == "external_http"
    assert "package_name" not in recorded["metadata"]
    assert "run_id" not in recorded["metadata"]
    assert listed[0]["progress_id"] == recorded["progress_id"]


def test_http_lists_job_tasks(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-tasks",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    client = cast(Any, TestClient(create_app(settings)))

    response = client.get(f"/jobs/{job.job_id}/tasks")

    assert response.status_code == 200
    assert response.json()[0]["task_id"] == task.task_id
    assert response.json()[0]["name"] == "jarvis.execution"


def test_http_task_timeline_events_are_replayable(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="http-task-events",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))
    client = cast(Any, TestClient(create_app(settings)))

    created = client.post(
        f"/tasks/{task.task_id}/events",
        json={
            "event_type": "dataset_found",
            "label": "dataset",
            "status": "succeeded",
            "summary": "Found staged dataset",
            "path_refs": ["/mnt/common/datasets/example_001"],
            "metadata": {"dataset": "example_001"},
        },
    )
    replay = client.get(f"/tasks/{task.task_id}/events", params={"cursor": 1})

    assert created.status_code == 200
    assert replay.status_code == 200
    assert created.json()["seq"] == 1
    assert replay.json()[0]["event_type"] == "dataset_found"
    assert replay.json()[0]["metadata"]["dataset"] == "example_001"


def test_http_task_timeline_sse_replays_existing_events(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="http-task-events-sse",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))
    queue.append_task_event(
        TaskTimelineEvent(
            task_id=task.task_id,
            event_type="repo_scan",
            label="repo",
            summary="Scanned visualization repository",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    with client.stream(
        "GET",
        f"/tasks/{task.task_id}/events/sse",
        params={"poll_seconds": 0.01, "stop_after_replay": True},
    ) as response:
        body = response.read().decode("utf-8")

    assert response.status_code == 200
    assert "event: task_events" in body
    assert "repo_scan" in body


def test_http_task_timeline_rejects_invalid_cursor(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="http-task-events-invalid-cursor",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))
    client = cast(Any, TestClient(create_app(settings)))

    response = client.get(f"/tasks/{task.task_id}/events", params={"cursor": 0})

    assert response.status_code == 422


def test_http_gateway_session_lifecycle(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))

    created = client.post(
        "/gateway-sessions",
        json={
            "cluster": "test-cluster",
            "name": "live-service-example",
            "requested_resources": {"nodes": 1, "exclusive": True},
            "gateway": {"strategy": "ssh_forward", "remote_port": 11111},
        },
    )
    session_id = created.json()["session_id"]
    updated = client.patch(
        f"/gateway-sessions/{session_id}",
        json={
            "state": "ready",
            "scheduler_job_id": "12345",
            "node": "ares-comp-01",
            "gateway": {"strategy": "ssh_forward", "local_port": 5900},
            "metadata": {"dataset": "example_001"},
        },
    )
    listed = client.get("/gateway-sessions", params={"cluster": "test-cluster"})
    closed = client.post(f"/gateway-sessions/{session_id}/close")
    reopen = client.patch(f"/gateway-sessions/{session_id}", json={"state": "ready"})

    assert created.status_code == 200
    assert updated.status_code == 200
    assert listed.status_code == 200
    assert closed.status_code == 200
    assert updated.json()["state"] == GatewaySessionState.READY.value
    assert updated.json()["scheduler_job_id"] == "12345"
    assert listed.json()[0]["session_id"] == session_id
    assert closed.json()["state"] == GatewaySessionState.CLOSED.value
    assert reopen.status_code == 409


def test_http_job_status_includes_relay_queue_and_scheduler(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-status",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    queue.update_task_metadata(
        task.task_id,
        {
            "scheduler_status": {
                "scheduler": "slurm",
                "scheduler_job_id": "100",
                "phase": "pending",
            }
        },
    )
    client = cast(Any, TestClient(create_app(settings)))

    response = client.get(f"/jobs/{job.job_id}/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["relay_queue"]["position"] == 1
    assert payload["scheduler"][0]["status"]["phase"] == "pending"


def test_http_queue_management_routes(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="test-cluster",
            hostname="node",
            pid=123,
            metadata={"concurrency": 2},
        )
    )
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-queue-management",
        )
    )
    queue.acquire_next_job("endpoint-1", cluster="test-cluster", ttl_seconds=-1)
    client = cast(Any, TestClient(create_app(settings)))

    listed = client.get("/queue", params={"cluster": "test-cluster"})
    diagnosed = client.get("/queue/diagnostics", params={"cluster": "test-cluster"})
    workers = client.get("/workers", params={"cluster": "test-cluster"})
    cleanup = client.post(
        "/queue/cleanup-stale",
        params={"cluster": "test-cluster", "dry_run": False},
    )
    canceled = client.post(f"/queue/jobs/{job.job_id}/cancel")

    assert listed.status_code == 200
    assert diagnosed.status_code == 200
    assert workers.status_code == 200
    assert cleanup.status_code == 200
    assert canceled.status_code == 200
    assert listed.json()["count"] == 1
    assert diagnosed.json()["issues"][0]["code"] == "expired_lease"
    assert workers.json()["configured_concurrency"] == 2
    assert cleanup.json()["recovered_count"] == 1
    assert canceled.json()["scheduler_policy"] == "relay-only"


def test_http_healthz_does_not_require_token(tmp_path: Path) -> None:
    client = cast(
        Any,
        TestClient(
            create_app(
                RelaySettings(
                    core_dir=tmp_path / "core",
                    spool_dir=tmp_path / "spool",
                    api_token="secret-token",
                )
            )
        ),
    )

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"ok": True, "auth": True}


def test_http_cancel_job_records_cancel_request(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-cancel",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    response = client.post(f"/jobs/{job.job_id}/cancel")
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)

    assert response.status_code == 200
    assert response.json()["state"] == "canceled"
    assert [event.event_type for event in events][-2:] == [
        "job.cancel_requested",
        "job.canceled",
    ]


def test_http_monitor_rules(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-monitor",
        )
    )
    queue.append_event(job.job_id, "stdout.delta", "step 50", payload={"text": "step 50\n"})
    client = cast(Any, TestClient(create_app(settings)))

    create_response = client.post(
        "/monitor/rules",
        json=MonitorRule(job_id=job.job_id, pattern="step 50").model_dump(mode="json"),
    )
    list_response = client.get("/monitor/rules", params={"job_id": job.job_id})
    run_response = client.post("/monitor/run-once")

    assert create_response.status_code == 200
    assert list_response.status_code == 200
    assert list_response.json()[0]["job_id"] == job.job_id
    assert run_response.status_code == 200
    assert run_response.json()[0]["action"] == "emit_event"

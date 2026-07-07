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
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    MonitorRule,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
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
            "tool": "simulate",
            "arguments": {"case": "lammps", "steps": 100},
            "timeout_seconds": 30,
            "idempotency_key": "http-typed-mcp",
        },
    )

    assert jarvis_response.status_code == 200
    assert agent_response.status_code == 200
    assert mcp_response.status_code == 200
    jarvis = queue.get_job(jarvis_response.json()["job_id"])
    agent = queue.get_job(agent_response.json()["job_id"])
    mcp = queue.get_job(mcp_response.json()["job_id"])
    assert jarvis.kind == JobKind.JARVIS
    assert isinstance(jarvis.spec, JarvisRunSpec)
    assert agent.kind == JobKind.REMOTE_AGENT
    assert isinstance(agent.spec, RemoteAgentTaskSpec)
    assert agent.spec.prompt_path == prompt_path
    assert agent.spec.mcp_config_path == mcp_config_path
    assert agent.spec.model == "configured-model"
    assert mcp.kind == JobKind.MCP_CALL
    assert isinstance(mcp.spec, McpCallSpec)
    assert mcp.spec.arguments == {"case": "lammps", "steps": 100}


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

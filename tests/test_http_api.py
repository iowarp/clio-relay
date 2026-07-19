from __future__ import annotations

import hashlib
import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    RemoteMcpServerConfig,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.http_api import create_app
from clio_relay.jarvis_mcp import jarvis_cd_lock_binding_expectation
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpAdmissionAuthority,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
    MonitorRule,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    TaskTimelineEvent,
    utc_now,
)
from clio_relay.remote_mcp import (
    cache_entry_from_discovery_artifact,
    remote_mcp_registration_revision,
    remote_mcp_server_artifact_digest,
)
from clio_relay.session_api import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
    session_identity_document,
)
from clio_relay.spool import JobSpool
from clio_relay.storage_runtime import StorageManagedQueue


def _bind_owned_session_cluster_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    definition: ClusterDefinition | None = None,
) -> ClusterDefinition:
    """Bind one exact test cluster definition as owned-session process authority."""
    bound_definition = definition or ClusterDefinition(
        name="test-cluster",
        ssh_host="test-cluster",
    )
    registry_path = tmp_path / "session-authority" / "clusters.json"
    ClusterRegistry(clusters={bound_definition.name: bound_definition}).save(registry_path)
    payload = registry_path.read_bytes()
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, str(registry_path))
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_REGISTRY_SHA256",
        hashlib.sha256(payload).hexdigest(),
    )
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_ROUTE_REVISION",
        cluster_route_revision(bound_definition),
    )
    return bound_definition


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
    invalid_log_responses = [
        client.get(f"/jobs/{job.job_id}/logs/stdout", params={"offset": -1}),
        client.get(f"/jobs/{job.job_id}/logs/stdout", params={"limit": 0}),
        client.get(f"/jobs/{job.job_id}/logs/stdout", params={"limit": 1_048_577}),
    ]

    assert monitor_response.status_code == 200
    assert monitor_response.json()["job"]["job_id"] == job.job_id
    assert log_response.status_code == 200
    assert log_response.json()["text"] == "hello"
    assert artifact_response.status_code == 200
    assert artifact_response.json()["artifact"]["artifact_id"] == artifact.artifact_id
    assert [response.status_code for response in invalid_log_responses] == [422, 422, 422]


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


def test_http_event_and_monitor_limits_reject_huge_values_before_queue_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-huge-page",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="bounded.events"))
    client = cast(Any, TestClient(create_app(settings)))

    def unexpected_read(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("query validation must run before queue access")

    monkeypatch.setattr(ClioCoreQueue, "get_job", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "get_task", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "drain_events", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "drain_task_events", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "list_monitor_rules", unexpected_read)

    requests = [
        client.get(f"/jobs/{job.job_id}/events", params={"limit": 10**12}),
        client.get(f"/tasks/{task.task_id}/events", params={"limit": 10**12}),
        client.get(f"/tasks/{task.task_id}/events/sse", params={"limit": 10**12}),
        client.get(f"/jobs/{job.job_id}/monitor", params={"limit": 10**12}),
        client.get(f"/jobs/{job.job_id}/monitor/sse", params={"limit": 10**12}),
        client.post("/monitor/run-once", params={"limit": 10**12}),
    ]

    assert [response.status_code for response in requests] == [422] * len(requests)


def test_http_websocket_limits_reject_huge_values_before_accept_or_queue_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-huge-websocket-page",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="bounded.websocket"))
    client = cast(Any, TestClient(create_app(settings)))

    def unexpected_read(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("WebSocket validation must run before queue access")

    monkeypatch.setattr(ClioCoreQueue, "get_job", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "get_task", unexpected_read)

    paths = [
        f"/jobs/{job.job_id}/monitor/ws?limit={10**12}",
        f"/tasks/{task.task_id}/events/ws?limit={10**12}",
    ]
    for path in paths:
        with pytest.raises(WebSocketDisconnect) as caught, client.websocket_connect(path):
            pass
        assert caught.value.code == 1008


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
            "pipeline_name": "site_simulation_4node",
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
            "env_from": {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"},
            "tool": "simulate",
            "arguments": {"case": "site-simulation", "steps": 100},
            "timeout_seconds": 30,
            "idempotency_key": "http-typed-mcp",
        },
    )
    jarvis_mcp_response = client.post(
        "/jobs/jarvis-mcp-call",
        json={
            "cluster": "test-cluster",
            "tool": "jarvis_describe",
            "arguments": {"target": "packages"},
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
    assert jarvis_pipeline.spec.pipeline_name == "site_simulation_4node"
    assert agent.kind == JobKind.REMOTE_AGENT
    assert isinstance(agent.spec, RemoteAgentTaskSpec)
    assert agent.spec.prompt_path == str(prompt_path)
    assert agent.spec.mcp_config_path == str(mcp_config_path)
    assert agent.spec.model == "configured-model"
    assert mcp.kind == JobKind.MCP_CALL
    assert isinstance(mcp.spec, McpCallSpec)
    assert mcp.spec.server_args == ["--stdio"]
    assert mcp.spec.env_from == {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    assert mcp.spec.arguments == {"case": "site-simulation", "steps": 100}
    assert isinstance(jarvis_mcp.spec, McpCallSpec)
    assert jarvis_mcp.spec.server == "clio-kit"
    assert jarvis_mcp.spec.server_args == ["mcp-server", "jarvis"]
    assert jarvis_mcp.spec.tool == "jarvis_describe"
    assert jarvis_mcp.spec.arguments == {"target": "packages"}


def test_http_mcp_admission_is_server_owned_and_raw_bypass_is_closed(
    tmp_path: Path,
) -> None:
    """Reject caller lane claims and keep arbitrary tools/list on workload capacity."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))
    raw_mcp = RelayJob(
        cluster="test-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(server="arbitrary-mcp", tool="inspect"),
        idempotency_key="raw-mcp-bypass",
    )
    forged_metadata = RelayJob(
        cluster="test-cluster",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="raw-authority-bypass",
        metadata={MCP_ADMISSION_AUTHORITY_METADATA_KEY: {"source": "forged"}},
    )

    raw_response = client.post("/jobs", json=raw_mcp.model_dump(mode="json"))
    metadata_response = client.post(
        "/jobs",
        json=forged_metadata.model_dump(mode="json"),
    )
    generic_enum = client.post(
        "/jobs/mcp-call",
        json={
            "cluster": "test-cluster",
            "server": "arbitrary-mcp",
            "tool": "inspect",
            "admission_class": "control_query",
            "idempotency_key": "forged-generic-enum",
        },
    )
    jarvis_enum = client.post(
        "/jobs/jarvis-mcp-call",
        json={
            "cluster": "test-cluster",
            "tool": "jarvis_describe",
            "admission_class": "control_query",
            "idempotency_key": "forged-jarvis-enum",
        },
    )
    arbitrary_discovery = client.post(
        "/jobs/mcp-call",
        json={
            "cluster": "test-cluster",
            "server": "arbitrary-mcp",
            "server_args": ["--hang"],
            "operation": "tools/list",
            "timeout_seconds": 1,
            "idempotency_key": "arbitrary-tools-list",
        },
    )

    assert raw_response.status_code == 422
    assert "must use /jobs/mcp-call" in raw_response.json()["detail"]
    assert metadata_response.status_code == 422
    assert "server-managed" in metadata_response.json()["detail"]
    assert generic_enum.status_code == 422
    assert jarvis_enum.status_code == 422
    assert arbitrary_discovery.status_code == 200
    queued = ClioCoreQueue(settings.core_dir).get_job(arbitrary_discovery.json()["job_id"])
    assert isinstance(queued.spec, McpCallSpec)
    assert queued.spec.operation is McpOperation.TOOLS_LIST
    assert queued.spec.admission_class is McpAdmissionClass.WORKLOAD
    assert MCP_ADMISSION_AUTHORITY_METADATA_KEY not in queued.metadata


def test_http_registered_tools_list_gets_bounded_intrinsic_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Promote only an exact enabled registration and persist server provenance."""
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        env_from={"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"},
        allow_tools=["inspect"],
        call_timeout_seconds=30,
    )
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(
        clusters={
            "alpha": ClusterDefinition(
                name="alpha",
                ssh_host="alpha-login",
                remote_mcp_servers={"science": registration},
            )
        }
    ).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))
    payload = {
        "cluster": "alpha",
        "server": registration.command,
        "server_args": registration.args,
        "env_from": registration.env_from,
        "operation": "tools/list",
        "timeout_seconds": registration.call_timeout_seconds,
        "idempotency_key": "registered-tools-list",
    }

    accepted = client.post("/jobs/mcp-call", json=payload)
    omitted_payload = {
        **payload,
        "idempotency_key": "registered-tools-list-omitted",
    }
    omitted_payload.pop("timeout_seconds")
    omitted = client.post(
        "/jobs/mcp-call",
        json=omitted_payload,
    )
    overlong = client.post(
        "/jobs/mcp-call",
        json={
            **payload,
            "timeout_seconds": registration.call_timeout_seconds + 1,
            "idempotency_key": "registered-tools-list-overlong",
        },
    )

    assert accepted.status_code == 200
    assert omitted.status_code == 409
    assert "explicit timeout" in omitted.json()["detail"]
    assert overlong.status_code == 409
    queued = ClioCoreQueue(settings.core_dir).get_job(accepted.json()["job_id"])
    assert isinstance(queued.spec, McpCallSpec)
    assert queued.spec.admission_class is McpAdmissionClass.CONTROL_QUERY
    authority = McpAdmissionAuthority.model_validate(
        queued.metadata[MCP_ADMISSION_AUTHORITY_METADATA_KEY]
    )
    assert authority.source == "intrinsic_tools_list"
    assert authority.operation is McpOperation.TOOLS_LIST


def test_owned_jarvis_mcp_submission_inherits_operator_spack_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cluster API, not the desktop process, binds the site Spack executable."""

    monkeypatch.setenv(
        "JARVIS_MCP_SPACK_COMMAND",
        "/opt/site-profiles/ares/bin/spack",
    )
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    client = cast(Any, TestClient(create_app(settings)))

    response = client.post(
        "/jobs/jarvis-mcp-call",
        json={
            "cluster": "test-cluster",
            "tool": "jarvis_run",
            "arguments": {
                "pipeline_id": "simulation",
                "spack_specs": ["lammps"],
            },
            "idempotency_key": "http-site-spack-reference",
        },
    )

    assert response.status_code == 200
    job = queue.get_job(response.json()["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.env_from == {"JARVIS_MCP_SPACK_COMMAND": "JARVIS_MCP_SPACK_COMMAND"}


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
                "adapter": "site-progress",
                "package_name": "site.simulation",
                "package_version": "2.1",
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
    assert listed["progress"][0]["progress_id"] == recorded["progress_id"]
    assert listed["cursor"] == 1
    assert listed["limit"] == 100
    assert listed["next_cursor"] is None
    assert listed["total"] == 1


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
    page = response.json()
    assert page["tasks"][0]["task_id"] == task.task_id
    assert page["tasks"][0]["name"] == "jarvis.execution"
    assert page["cursor"] == 1
    assert page["limit"] == 100
    assert page["next_cursor"] is None
    assert page["total"] == 1


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
    assert updated.json()["scheduler"] == "external"
    assert updated.json()["scheduler_job_id"] is None
    listed_page = listed.json()
    assert listed_page["gateway_sessions"][0]["session_id"] == session_id
    assert listed_page["source_cursor"] == 1
    assert listed_page["source_limit"] == 100
    assert listed_page["source_next_cursor"] is None
    assert listed_page["source_total"] == 1
    assert closed.json()["state"] == GatewaySessionState.CLOSED.value
    assert reopen.status_code == 409


@pytest.mark.parametrize(
    "payload",
    [
        {"scheduler": "slurm"},
        {"scheduler_job_id": "12345"},
        {"gateway": {"runtime_spec": {"kind": "forged"}}},
        {"gateway": {"jarvis_runtime_binding": {"schema_version": "forged"}}},
        {"gateway": {"scheduler_job_id": "12345"}},
        {"gateway": {"ownership_intents": {"scheduler_submission": {}}}},
        {"gateway": {"transport": {"remote_connector": {"pid": 42}}}},
        {"metadata": {"owner": "clio-relay"}},
        {"metadata": {"scheduler_provider": "slurm"}},
        {"metadata": {"owner_session_generation_id": "forged-generation"}},
    ],
)
def test_http_generic_gateway_create_rejects_relay_runtime_ownership_fields(
    tmp_path: Path,
    payload: dict[str, object],
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))

    response = client.post(
        "/gateway-sessions",
        json={"cluster": "test-cluster", "name": "forged-runtime", **payload},
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    "payload",
    [
        {"scheduler_job_id": "12345"},
        {"gateway": {"runtime_spec": {"kind": "forged"}}},
        {"gateway": {"jarvis_runtime_binding": {"schema_version": "forged"}}},
        {"gateway": {"transport": {"desktop_connector": {"pid": 42}}}},
        {"metadata": {"owner_session_id": "forged-session"}},
    ],
)
def test_http_generic_gateway_update_rejects_relay_runtime_ownership_fields(
    tmp_path: Path,
    payload: dict[str, object],
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))
    created = client.post(
        "/gateway-sessions",
        json={"cluster": "test-cluster", "name": "ordinary-gateway"},
    )

    response = client.patch(
        f"/gateway-sessions/{created.json()['session_id']}",
        json=payload,
    )

    assert created.status_code == 200
    assert response.status_code == 422


def test_http_generic_gateway_update_cannot_replace_relay_managed_runtime_state(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    runtime = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="relay-managed-runtime",
            gateway={
                "runtime_spec": {"kind": "image-service"},
                "ownership_intents": {"scheduler_submission": {"state": "recorded"}},
            },
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    response = client.patch(
        f"/gateway-sessions/{runtime.session_id}",
        json={"gateway": {"strategy": "ssh_forward"}},
    )

    assert response.status_code == 409
    assert queue.get_gateway_session(runtime.session_id).gateway == runtime.gateway


def test_owned_session_api_fails_closed_without_cluster_or_owner_token(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="OWNER_SESSION_CLUSTER"):
        create_app(
            RelaySettings(
                core_dir=tmp_path / "core",
                spool_dir=tmp_path / "spool",
                api_token="session-api-token",
                owner_session_id="desktop-session-1",
                owner_session_generation_id="generation-1",
            )
        )
    with pytest.raises(ConfigurationError, match="SESSION_OWNER_TOKEN"):
        create_app(
            RelaySettings(
                core_dir=tmp_path / "core",
                spool_dir=tmp_path / "spool",
                api_token="session-api-token",
                owner_session_id="desktop-session-1",
                owner_session_generation_id="generation-1",
                owner_session_cluster="test-cluster",
            )
        )
    with pytest.raises(ConfigurationError, match="at least 32 bytes"):
        create_app(
            RelaySettings(
                core_dir=tmp_path / "core",
                spool_dir=tmp_path / "spool",
                api_token="session-api-token",
                owner_session_id="desktop-session-1",
                owner_session_generation_id="generation-1",
                owner_session_cluster="test-cluster",
                session_owner_token="weak-token",
            )
        )


def test_owned_session_api_fails_closed_without_exact_process_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
    )

    with pytest.raises(ConfigurationError, match="process-bound cluster authority"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    monkeypatch.delenv("CLIO_RELAY_SESSION_ROUTE_REVISION")
    with pytest.raises(ConfigurationError, match="must be configured together"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, "")
    with pytest.raises(ConfigurationError, match="path must not be blank"):
        create_app(settings)


def test_owned_session_api_rejects_invalid_or_ambiguous_process_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="test-cluster", ssh_host="test-cluster")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
    )

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    monkeypatch.setenv("CLIO_RELAY_SESSION_REGISTRY_SHA256", "invalid")
    with pytest.raises(ConfigurationError, match="registry SHA-256 is invalid"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    monkeypatch.setenv("CLIO_RELAY_SESSION_ROUTE_REVISION", "invalid")
    with pytest.raises(ConfigurationError, match="route revision is invalid"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    monkeypatch.setenv("CLIO_RELAY_SESSION_REGISTRY_SHA256", "f" * 64)
    with pytest.raises(ConfigurationError, match="registry digest does not match"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    monkeypatch.setenv("CLIO_RELAY_SESSION_ROUTE_REVISION", "f" * 64)
    with pytest.raises(ConfigurationError, match="route revision does not match"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    registry_path = Path(os.environ[CLUSTER_REGISTRY_ENV])
    ClusterRegistry(
        clusters={
            "test-cluster": definition,
            "other-cluster": ClusterDefinition(
                name="other-cluster",
                ssh_host="other-cluster",
            ),
        }
    ).save(registry_path)
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_REGISTRY_SHA256",
        hashlib.sha256(registry_path.read_bytes()).hexdigest(),
    )
    with pytest.raises(ConfigurationError, match="exactly the owner session cluster"):
        create_app(settings)


def test_normal_desktop_api_accepts_operator_registry_without_session_markers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        allow_tools=["inspect"],
        profiles=["user"],
        call_timeout_seconds=30,
    )
    definition = ClusterDefinition(
        name="alpha",
        ssh_host="alpha-login",
        remote_mcp_servers={"science": registration},
    )
    registry_path = tmp_path / "desktop-registry" / "clusters.json"
    ClusterRegistry(clusters={"alpha": definition}).save(registry_path)
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, str(registry_path))
    monkeypatch.delenv("CLIO_RELAY_SESSION_REGISTRY_SHA256", raising=False)
    monkeypatch.delenv("CLIO_RELAY_SESSION_ROUTE_REVISION", raising=False)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    client = cast(Any, TestClient(create_app(settings)))

    response = client.post(
        "/jobs/mcp-call",
        json={
            "cluster": "alpha",
            "server": registration.command,
            "server_args": registration.args,
            "operation": "tools/list",
            "timeout_seconds": 30,
            "idempotency_key": "desktop-science-discovery",
        },
    )

    assert response.status_code == 200
    job = queue.get_job(response.json()["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.admission_class is McpAdmissionClass.CONTROL_QUERY


def test_owned_registered_mcp_call_uses_immutable_session_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        env_from={"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"},
        allow_tools=["inspect"],
        profiles=["user"],
        call_timeout_seconds=30,
        schema_cache_ttl_seconds=3600,
    )
    definition = ClusterDefinition(
        name="alpha",
        ssh_host="alpha-login",
        remote_mcp_servers={"science": registration},
    )
    _bind_owned_session_cluster_authority(
        monkeypatch,
        tmp_path,
        definition=definition,
    )
    registry_path = Path(os.environ[CLUSTER_REGISTRY_ENV])
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="alpha",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    discovery = queue.submit_job(
        RelayJob(
            cluster="alpha",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=registration.command,
                server_args=registration.args,
                env_from=registration.env_from,
                operation=McpOperation.TOOLS_LIST,
                admission_class=McpAdmissionClass.CONTROL_QUERY,
            ),
            idempotency_key="owned-http-science-discovery",
        )
    )
    server_artifact = {
        "verified": True,
        "server_process_artifact_verified": True,
        "executable": {
            "path": "/opt/science/bin/science-mcp",
            "sha256": "a" * 64,
        },
    }
    discovery_payload = json.dumps(
        {
            "server": registration.command,
            "server_args": registration.args,
            "env_from": registration.env_from,
            "operation": "tools/list",
            "tool": None,
            "arguments": {},
            "protocol_result": {
                "tools": [
                    {
                        "name": "inspect",
                        "description": "Inspect one scientific dataset.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"path": {"type": "string"}},
                            "required": ["path"],
                            "additionalProperties": False,
                        },
                        "annotations": {
                            "readOnlyHint": True,
                            "destructiveHint": False,
                        },
                    }
                ]
            },
            "structured_result": None,
            "protocol_version": "2024-11-05",
            "server_info": {"name": "science", "version": "1.2.3"},
            "server_artifact": server_artifact,
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "timed_out": False,
            "protocol_error": None,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    spool = JobSpool(settings.spool_dir, discovery)
    spool.initialize()
    result_path = spool.path / "mcp-result.json"
    result_path.write_bytes(discovery_payload)
    artifact = queue.append_artifact(spool.artifact_for(result_path, kind="mcp_result"))
    assert artifact.sha256 is not None
    artifact_sha256 = artifact.sha256
    discovery = queue.update_job_state(discovery.job_id, JobState.SUCCEEDED)
    entry = cache_entry_from_discovery_artifact(
        cluster="alpha",
        server_name="science",
        registration=registration,
        discovery_job_id=discovery.job_id,
        artifact_id=artifact.artifact_id,
        artifact_sha256=artifact_sha256,
        artifact_payload=discovery_payload,
        discovered_at=discovery.updated_at,
    )
    server_digest = remote_mcp_server_artifact_digest(entry.provenance.server_artifact)
    evidence = McpControlQueryEvidence(
        cluster="alpha",
        registered_server_name="science",
        cluster_route_revision=cluster_route_revision(definition),
        registration_revision=remote_mcp_registration_revision(registration),
        discovery_job_id=discovery.job_id,
        discovery_artifact_id=artifact.artifact_id,
        discovery_artifact_sha256=artifact_sha256,
        discovery_schema_digest=entry.schema_digest,
        expected_server_artifact_digest=server_digest,
    )
    app = create_app(settings)
    changed_definition = definition.model_copy(update={"ssh_host": "changed-login"})
    ClusterRegistry(clusters={"alpha": changed_definition}).save(registry_path)
    client = cast(
        Any,
        TestClient(
            app,
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )
    request = {
        "cluster": "alpha",
        "server": registration.command,
        "server_args": registration.args,
        "env_from": registration.env_from,
        "expected_server_artifact_digest": server_digest,
        "operation": "tools/call",
        "tool": "inspect",
        "arguments": {"path": "/datasets/example.bp"},
        "control_query_evidence": evidence.model_dump(mode="json"),
        "timeout_seconds": 30,
        "idempotency_key": "owned-http-science-inspect",
    }

    accepted = client.post("/jobs/mcp-call", json=request)
    assert accepted.status_code == 200
    accepted_job = queue.get_job(accepted.json()["job_id"])
    assert isinstance(accepted_job.spec, McpCallSpec)
    assert accepted_job.spec.admission_class is McpAdmissionClass.CONTROL_QUERY
    authority = McpAdmissionAuthority.model_validate(
        accepted_job.metadata[MCP_ADMISSION_AUTHORITY_METADATA_KEY]
    )
    assert authority.source == "registered_discovery_artifact"
    assert authority.evidence == evidence

    route_drift = client.post(
        "/jobs/mcp-call",
        json={
            **request,
            "idempotency_key": "owned-http-science-route-drift",
            "control_query_evidence": {
                **evidence.model_dump(mode="json"),
                "cluster_route_revision": cluster_route_revision(changed_definition),
            },
        },
    )
    assert route_drift.status_code == 409
    assert "cluster route changed" in route_drift.json()["detail"]

    registration_drift = client.post(
        "/jobs/mcp-call",
        json={
            **request,
            "idempotency_key": "owned-http-science-registration-drift",
            "control_query_evidence": {
                **evidence.model_dump(mode="json"),
                "registration_revision": "f" * 64,
            },
        },
    )
    assert registration_drift.status_code == 409
    assert "registered MCP server changed" in registration_drift.json()["detail"]

    with pytest.raises(ConfigurationError, match="registry digest does not match"):
        create_app(settings)


def test_owned_session_identity_challenge_is_public_and_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    client = cast(Any, TestClient(create_app(settings)))
    nonce = "1" * 64

    response = client.get("/session-identity", params={"nonce": nonce})

    assert response.status_code == 200
    assert response.json() == session_identity_document(
        owner_token="o" * 32,
        cluster="test-cluster",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce=nonce,
    )
    assert client.get("/session-identity", params={"nonce": "not-a-nonce"}).status_code == 422


def test_owned_session_api_stamps_jobs_and_gateways_with_server_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    assert (
        queue.prepare_owner_session_start(
            "desktop-session-1",
            recorded_generation_id=None,
            candidate_generation_id="generation-1",
        )
        == "generation-1"
    )
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )
    raw_job = RelayJob(
        cluster="test-cluster",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["sleep", "60"]),
        idempotency_key="owned-http-job",
    )

    submitted = client.post("/jobs", json=raw_job.model_dump(mode="json"))
    forged_payload = raw_job.model_dump(mode="json")
    forged_payload["metadata"] = {
        "owner": "untrusted-client",
        "owner_session_id": "forged-session",
        "owner_session_generation_id": "forged-generation",
        "owner_session_admission_id": "forged-admission",
    }
    forged = client.post("/jobs", json=forged_payload)
    gateway = client.post(
        "/gateway-sessions",
        json={
            "cluster": "test-cluster",
            "name": "owned-gateway",
            "metadata": {"dataset": "example"},
        },
    )
    patched = client.patch(
        f"/gateway-sessions/{gateway.json()['session_id']}",
        json={"metadata": {"phase": "ready"}},
    )

    assert submitted.status_code == 200
    assert forged.status_code == 422
    assert "server-managed" in forged.json()["detail"]
    assert submitted.json()["metadata"] == {
        "owner": "clio-relay",
        "owner_session_id": "desktop-session-1",
        "owner_session_generation_id": "generation-1",
    }
    assert gateway.status_code == 200
    assert patched.status_code == 200
    assert gateway.json()["metadata"]["owner_session_generation_id"] == "generation-1"
    assert patched.json()["metadata"]["owner"] == "clio-relay"
    assert patched.json()["metadata"]["owner_session_id"] == "desktop-session-1"
    assert patched.json()["metadata"]["owner_session_generation_id"] == "generation-1"
    assert patched.json()["metadata"]["dataset"] == "example"
    assert patched.json()["metadata"]["phase"] == "ready"


def test_session_job_submission_rejects_missing_stale_and_unbound_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    client = cast(Any, TestClient(create_app(settings)))
    payload = {
        "cluster": "test-cluster",
        "pipeline_name": "pipeline",
        "idempotency_key": "session-header-adversarial",
    }
    authorization = {"Authorization": "Bearer session-api-token"}

    missing = client.post("/jobs/jarvis-pipeline", headers=authorization, json=payload)
    stale = client.post(
        "/jobs/jarvis-pipeline",
        headers={
            **authorization,
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-0",
        },
        json=payload,
    )
    wrong_session = client.post(
        "/jobs/jarvis-pipeline",
        headers={
            **authorization,
            OWNER_SESSION_ID_HEADER: "desktop-session-2",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
        json=payload,
    )
    gateway_payload = {"cluster": "test-cluster", "name": "bound-gateway"}
    missing_gateway = client.post(
        "/gateway-sessions",
        headers=authorization,
        json=gateway_payload,
    )
    stale_gateway = client.post(
        "/gateway-sessions",
        headers={
            **authorization,
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-0",
        },
        json=gateway_payload,
    )
    wrong_cluster_gateway = client.post(
        "/gateway-sessions",
        headers={
            **authorization,
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
        json={"cluster": "other-cluster", "name": "wrong-cluster"},
    )

    assert missing.status_code == 409
    assert stale.status_code == 409
    assert wrong_session.status_code == 409
    assert missing_gateway.status_code == 409
    assert stale_gateway.status_code == 409
    assert wrong_cluster_gateway.status_code == 409
    assert queue.list_jobs() == []
    assert queue.list_gateway_sessions() == []

    monkeypatch.delenv(CLUSTER_REGISTRY_ENV)
    monkeypatch.delenv("CLIO_RELAY_SESSION_REGISTRY_SHA256")
    monkeypatch.delenv("CLIO_RELAY_SESSION_ROUTE_REVISION")
    unbound_settings = RelaySettings(
        core_dir=tmp_path / "unbound-core",
        spool_dir=tmp_path / "unbound-spool",
        api_token="session-api-token",
    )
    unbound = cast(Any, TestClient(create_app(unbound_settings))).post(
        "/jobs/jarvis-pipeline",
        headers={
            **authorization,
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
        json=payload,
    )
    assert unbound.status_code == 409
    assert "not bound" in unbound.json()["detail"]


def test_owned_jarvis_mcp_submission_forwards_desktop_binding_without_remote_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    expected_digest = "a" * 64
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    unrelated = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="unrelated-sentinel-job",
        )
    )

    def fail_on_cluster_cache(_cluster: str) -> str:
        pytest.fail("owned API must not consult its cluster-local cache")

    monkeypatch.setattr(
        "clio_relay.http_api.jarvis_mcp_artifact_binding",
        fail_on_cluster_cache,
    )
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )
    payload = {
        "cluster": "test-cluster",
        "tool": "jarvis_get_execution",
        "arguments": {
            "pipeline_id": "pipeline",
            "execution_id": "execution-1",
            "include_service_runtimes": True,
        },
        "idempotency_key": "owned-artifact-source",
    }

    omitted = client.post("/jobs/jarvis-mcp-call", json=payload)
    accepted = client.post(
        "/jobs/jarvis-mcp-call",
        json={**payload, "expected_server_artifact_digest": expected_digest},
    )
    oversized = client.post(
        "/jobs/jarvis-mcp-call",
        json={
            **payload,
            "expected_server_artifact_digest": expected_digest,
            "timeout_seconds": 61,
            "idempotency_key": "owned-artifact-source-oversized",
        },
    )

    assert omitted.status_code == 422
    assert accepted.status_code == 200
    assert oversized.status_code == 422
    assert "timeout exceeds 60 seconds" in str(oversized.json())
    job = queue.get_job(accepted.json()["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.expected_server_artifact_digest == expected_digest
    assert job.spec.expected_jarvis_cd_lock_binding == jarvis_cd_lock_binding_expectation()
    assert job.spec.timeout_seconds == 60
    metadata = dict(job.metadata)
    authority = McpAdmissionAuthority.model_validate(
        metadata.pop(MCP_ADMISSION_AUTHORITY_METADATA_KEY)
    )
    assert authority.source == "pinned_jarvis_contract"
    assert authority.tool == "jarvis_get_execution"
    assert authority.expected_server_artifact_digest == expected_digest
    assert metadata == {
        "owner": "clio-relay",
        "owner_session_id": "desktop-session-1",
        "owner_session_generation_id": "generation-1",
    }
    memberships, next_cursor, total, scanned = queue.list_owner_session_jobs_page(
        "desktop-session-1",
        session_generation_id="generation-1",
        limit=100,
    )
    assert [membership.job_id for membership in memberships] == [job.job_id]
    assert next_cursor is None
    assert total == 1
    assert scanned == 1

    canceled = client.post(
        f"/jobs/{job.job_id}/cancel",
        json={"cluster": "test-cluster", "cancel_scheduler_job": True},
    )
    assert canceled.status_code == 200
    assert canceled.json()["state"] == "canceled"
    assert queue.get_job(unrelated.job_id).state is JobState.QUEUED


def test_unowned_jarvis_mcp_submission_still_validates_operator_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-owned API continues to enforce its own operator discovery cache."""

    expected_digest = "a" * 64
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    def artifact_binding(_cluster: str) -> str:
        return expected_digest

    monkeypatch.setattr(
        "clio_relay.http_api.jarvis_mcp_artifact_binding",
        artifact_binding,
    )
    client = cast(Any, TestClient(create_app(settings)))
    payload = {
        "cluster": "test-cluster",
        "tool": "jarvis_describe",
        "arguments": {"target": "packages"},
    }

    mismatched = client.post(
        "/jobs/jarvis-mcp-call",
        json={
            **payload,
            "idempotency_key": "unowned-mismatched-binding",
            "expected_server_artifact_digest": "b" * 64,
        },
    )
    accepted = client.post(
        "/jobs/jarvis-mcp-call",
        json={
            **payload,
            "idempotency_key": "unowned-exact-binding",
            "expected_server_artifact_digest": expected_digest,
        },
    )

    assert mismatched.status_code == 409
    assert accepted.status_code == 200
    job = queue.get_job(accepted.json()["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.expected_server_artifact_digest == expected_digest
    assert job.spec.expected_jarvis_cd_lock_binding == jarvis_cd_lock_binding_expectation()


def test_owned_session_submission_race_with_quiesce_returns_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    original_submit = StorageManagedQueue.submit_job

    def quiesce_before_commit(self: StorageManagedQueue, job: RelayJob) -> RelayJob:
        self.set_owner_session_closing(
            "desktop-session-1",
            session_generation_id="generation-1",
        )
        return original_submit(self, job)

    monkeypatch.setattr(StorageManagedQueue, "submit_job", quiesce_before_commit)
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )

    response = client.post(
        "/jobs/jarvis-pipeline",
        json={
            "cluster": "test-cluster",
            "pipeline_name": "pipeline",
            "idempotency_key": "quiesce-race",
        },
    )

    assert response.status_code == 409
    assert queue.list_jobs() == []


def test_owned_session_api_cannot_take_over_or_close_other_gateways(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    assert (
        queue.prepare_owner_session_start(
            "desktop-session-2",
            recorded_generation_id=None,
            candidate_generation_id="generation-2",
        )
        == "generation-2"
    )
    other_owned = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="other-owned",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-2",
                "owner_session_generation_id": "generation-2",
            },
        )
    )
    assert (
        queue.prepare_owner_session_start(
            "desktop-session-1",
            recorded_generation_id=None,
            candidate_generation_id="generation-0",
        )
        == "generation-0"
    )
    prior_generation = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="prior-generation",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-1",
                "owner_session_generation_id": "generation-0",
            },
        )
    )
    prior_generation = queue.close_gateway_session(prior_generation.session_id)
    queue.set_owner_session_closing(
        "desktop-session-1",
        session_generation_id="generation-0",
    )
    queue.set_owner_session_closed(
        "desktop-session-1",
        session_generation_id="generation-0",
    )
    assert (
        queue.prepare_owner_session_start(
            "desktop-session-1",
            recorded_generation_id="generation-0",
            candidate_generation_id="generation-1",
        )
        == "generation-1"
    )
    unowned = queue.create_gateway_session(GatewaySession(cluster="test-cluster", name="unowned"))
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )

    for session in (other_owned, prior_generation, unowned):
        patched = client.patch(
            f"/gateway-sessions/{session.session_id}",
            json={"metadata": {"phase": "forged"}},
        )
        closed = client.post(f"/gateway-sessions/{session.session_id}/close")

        assert patched.status_code == 403
        assert closed.status_code == 403
        unchanged = queue.get_gateway_session(session.session_id)
        assert unchanged.state is session.state
        assert unchanged.metadata == session.metadata


def test_owned_session_api_filters_jobs_redacts_capabilities_and_quiesces_intake(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    owned = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="owned-session-visible",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-1",
                "owner_session_generation_id": "generation-1",
                "owner_token": "private-capability",
            },
        )
    )
    other = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="other-session-hidden",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-2",
                "owner_session_generation_id": "generation-2",
            },
        )
    )
    prior_generation = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="prior-generation-hidden",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-1",
                "owner_session_generation_id": "generation-0",
            },
        )
    )
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )

    owned_response = client.get(f"/jobs/{owned.job_id}")
    other_response = client.get(f"/jobs/{other.job_id}")
    other_status = client.get(f"/jobs/{other.job_id}/status")
    prior_generation_response = client.get(f"/jobs/{prior_generation.job_id}")
    listing = client.get("/queue")

    assert owned_response.status_code == 200
    assert owned_response.json()["metadata"]["owner_token"] == "<redacted>"
    assert queue.get_job(owned.job_id).metadata["owner_token"] == "private-capability"
    assert other_response.status_code == 403
    assert other_status.status_code == 403
    assert prior_generation_response.status_code == 403
    assert listing.status_code == 200
    assert listing.json()["count"] == 1
    assert listing.json()["jobs"][0]["job"]["job_id"] == owned.job_id
    assert client.get(f"/queue/jobs/{owned.job_id}/diagnose").status_code == 200
    assert client.get(f"/queue/jobs/{other.job_id}/diagnose").status_code == 403
    assert client.get("/queue/diagnostics").status_code == 403
    assert client.get("/queue/stale", params={"cluster": "test-cluster"}).status_code == 403
    assert client.get("/workers").status_code == 403

    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    queue.set_owner_session_closing(
        "desktop-session-1",
        session_generation_id="generation-1",
    )
    new_job = RelayJob(
        cluster="test-cluster",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="rejected-after-quiesce",
    )
    assert client.post("/jobs", json=new_job.model_dump(mode="json")).status_code == 409
    assert (
        client.post(
            "/gateway-sessions",
            json={"cluster": "test-cluster", "name": "rejected-after-quiesce"},
        ).status_code
        == 409
    )


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

    listed = client.get(
        "/queue",
        params={
            "cluster": "test-cluster",
            "kind": "jarvis",
            "limit": 1,
            "scan_limit": 1,
        },
    )
    diagnosed = client.get("/queue/diagnostics", params={"cluster": "test-cluster"})
    specific = client.get(
        f"/queue/jobs/{job.job_id}/diagnose",
        params={"cluster": "test-cluster", "older_than_seconds": 3600},
    )
    stale = client.get(
        "/queue/stale",
        params={
            "cluster": "test-cluster",
            "older_than_seconds": 3600,
            "kind": "jarvis",
        },
    )
    workers = client.get("/workers", params={"cluster": "test-cluster"})
    cleanup = client.post(
        "/queue/cleanup-stale",
        params={"cluster": "test-cluster", "dry_run": False},
    )
    canceled = client.post(f"/queue/jobs/{job.job_id}/cancel")

    assert listed.status_code == 200
    assert diagnosed.status_code == 200
    assert specific.status_code == 200
    assert stale.status_code == 200
    assert workers.status_code == 200
    assert cleanup.status_code == 200
    assert canceled.status_code == 200
    assert listed.json()["count"] == 1
    assert diagnosed.json()["issues"][0]["code"] == "expired_lease"
    assert specific.json()["reason"] == "stale_lease"
    assert stale.json()["jobs"][0]["job"]["job_id"] == job.job_id
    assert workers.json()["configured_concurrency"] == 2
    assert cleanup.json()["recovered_count"] == 1
    assert canceled.json()["scheduler_policy"] == "relay-only"


def test_http_queue_job_routes_reject_cluster_mismatch(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="http-cluster-mismatch",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    diagnosis = client.get(
        f"/queue/jobs/{job.job_id}/diagnose",
        params={"cluster": "homelab"},
    )
    canceled = client.post(
        f"/queue/jobs/{job.job_id}/cancel",
        json={"cluster": "homelab"},
    )

    assert diagnosis.status_code == 409
    assert canceled.status_code == 409
    assert queue.get_job(job.job_id).state == JobState.QUEUED


def test_http_stale_exact_job_target_preserves_neighbor(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    jobs = [
        queue.submit_job(
            RelayJob(
                cluster="test-cluster",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["true"]),
                idempotency_key=f"http-exact-stale-{index}",
            )
        )
        for index in range(2)
    ]
    old = utc_now() - timedelta(hours=3)
    for job in jobs:
        queue._write_job_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job.model_copy(update={"created_at": old, "updated_at": old})
        )
    client = cast(Any, TestClient(create_app(settings)))

    discovered = client.get(
        "/queue/stale",
        params={
            "cluster": "test-cluster",
            "job_id": jobs[0].job_id,
            "older_than_seconds": 60,
        },
    )
    cleaned = client.post(
        "/queue/cleanup-stale",
        params={
            "cluster": "test-cluster",
            "job_id": jobs[0].job_id,
            "older_than_seconds": 60,
            "cancel_queued": True,
            "dry_run": False,
        },
    )

    assert discovered.status_code == 200
    assert cleaned.status_code == 200
    assert [item["job"]["job_id"] for item in discovered.json()["jobs"]] == [jobs[0].job_id]
    assert [item["job_id"] for item in cleaned.json()["planned"]] == [jobs[0].job_id]
    assert queue.get_job(jobs[0].job_id).state is JobState.CANCELED
    assert queue.get_job(jobs[1].job_id).state is JobState.QUEUED


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
    assert list_response.json()["rules"][0]["job_id"] == job.job_id
    assert list_response.json()["source_cursor"] == 1
    assert list_response.json()["source_total"] == 1
    assert run_response.status_code == 200
    assert run_response.json()[0]["action"] == "emit_event"

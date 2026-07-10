from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from clio_relay import cli
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, FrpTransportConfig
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import (
    ArtifactRef,
    EndpointRegistration,
    EndpointRole,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    RelayTask,
)


@pytest.fixture(autouse=True)
def _default_cli_mode(monkeypatch: MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")


def _write_test_cluster(
    root: Path,
    name: str = "ares",
    *,
    frp_server_addr: str = "relay.example.test",
) -> None:
    ClusterRegistry(
        clusters={
            name: ClusterDefinition(
                name=name,
                ssh_host=name,
                frp_transport=FrpTransportConfig(server_addr=frp_server_addr),
            )
        }
    ).save(root / ".clio-relay" / "clusters.json")


def test_console_safe_text_replaces_non_console_characters(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "stdout", SimpleNamespace(encoding="cp1252"))

    assert cli._console_safe_text("× ╰─▶") == "× ???"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_cli_lists_artifacts(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    artifact_path = tmp_path / "stdout.log"
    artifact_path.write_text("hello\n", encoding="utf-8")
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-artifacts",
        )
    )
    artifact = queue.append_artifact(
        ArtifactRef(job_id=job.job_id, uri=artifact_path.as_uri(), kind="stdout")
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "list-artifacts", job.job_id])

    assert result.exit_code == 0
    artifacts = json.loads(result.output)
    assert artifacts[0]["artifact_id"] == artifact.artifact_id
    assert artifacts[0]["kind"] == "stdout"


def test_cli_lists_tasks(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-tasks",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "tasks", job.job_id])

    assert result.exit_code == 0
    tasks = json.loads(result.output)
    assert tasks[0]["task_id"] == task.task_id
    assert tasks[0]["name"] == "jarvis.execution"


def test_cli_records_and_reads_task_events(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-task-events",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    record = CliRunner().invoke(
        app,
        [
            "job",
            "record-task-event",
            task.task_id,
            "--event-type",
            "dataset_found",
            "--label",
            "dataset",
            "--summary",
            "Found staged dataset",
            "--status",
            "succeeded",
            "--path-ref",
            "/mnt/common/datasets/example_001",
        ],
    )
    read = CliRunner().invoke(app, ["job", "task-events", task.task_id])

    assert record.exit_code == 0
    assert read.exit_code == 0
    payload = json.loads(read.output)
    assert payload["events"][0]["event_type"] == "dataset_found"
    assert payload["events"][0]["path_refs"] == ["/mnt/common/datasets/example_001"]


def test_cli_job_watch_accepts_zero_cursor(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-watch-zero-cursor",
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "watch", job.job_id, "--cursor", "0"])

    assert result.exit_code == 0
    assert "job.queued" in result.output
    assert "next_cursor=2" in result.output


def test_cli_job_monitor_accepts_zero_cursor(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-monitor-zero-cursor",
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "monitor", job.job_id, "--cursor", "0"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["events"][0]["event_type"] == "job.queued"
    assert payload["next_cursor"] == 2


def test_cli_gateway_session_lifecycle(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    gateway_json = tmp_path / "gateway.json"
    gateway_json.write_text('{"strategy":"ssh_forward","remote_port":11111}', encoding="utf-8")
    resources_json = tmp_path / "resources.json"
    resources_json.write_text('{"nodes":1,"exclusive":true}', encoding="utf-8")

    created = CliRunner().invoke(
        app,
        [
            "gateway",
            "create",
            "--cluster",
            "test-cluster",
            "--name",
            "live-service-example",
            "--gateway-json-file",
            str(gateway_json),
            "--resources-json-file",
            str(resources_json),
            "--stdout-uri",
            "file:///tmp/stdout.log",
            "--stderr-uri",
            "file:///tmp/stderr.log",
            "--log-uri",
            "file:///tmp/service.log",
            "--artifact",
            "artifact://session/startup",
        ],
    )
    assert created.exit_code == 0
    session_id = json.loads(created.output)["session_id"]

    updated = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            session_id,
            "--state",
            "ready",
            "--scheduler-job-id",
            "12345",
            "--node",
            "ares-comp-01",
            "--gateway-json",
            '{"strategy":"ssh_forward","local_port":5900}',
            "--resources-json",
            '{"nodes":2}',
            "--stdout-uri",
            "file:///tmp/updated-stdout.log",
            "--log-uri",
            "file:///tmp/updated.log",
            "--artifact",
            "artifact://session/updated",
        ],
    )
    listed = CliRunner().invoke(app, ["gateway", "list", "--cluster", "test-cluster"])
    closed = CliRunner().invoke(app, ["gateway", "close", session_id])

    assert updated.exit_code == 0
    assert listed.exit_code == 0
    assert closed.exit_code == 0
    assert json.loads(updated.output)["state"] == GatewaySessionState.READY.value
    assert json.loads(created.output)["gateway"]["remote_port"] == 11111
    assert json.loads(created.output)["requested_resources"]["exclusive"] is True
    assert json.loads(created.output)["stdout_uri"] == "file:///tmp/stdout.log"
    assert json.loads(created.output)["log_uris"] == ["file:///tmp/service.log"]
    assert json.loads(created.output)["artifacts"] == ["artifact://session/startup"]
    assert json.loads(updated.output)["requested_resources"] == {"nodes": 2}
    assert json.loads(updated.output)["stdout_uri"] == "file:///tmp/updated-stdout.log"
    assert json.loads(updated.output)["log_uris"] == ["file:///tmp/updated.log"]
    assert json.loads(updated.output)["artifacts"] == ["artifact://session/updated"]
    assert json.loads(listed.output)[0]["session_id"] == session_id
    assert json.loads(closed.output)["state"] == GatewaySessionState.CLOSED.value


def test_cli_job_status_includes_relay_queue(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-status",
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "status", job.job_id])

    assert result.exit_code == 0
    status = json.loads(result.output)
    assert status["job"]["job_id"] == job.job_id
    assert status["relay_queue"] == {"state": "queued", "jobs_ahead": 0, "position": 1}


def test_cli_queue_management_commands(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-queue-management",
        )
    )
    queue.acquire_next_job("endpoint-1", cluster="test-cluster", ttl_seconds=-1)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    listed = runner.invoke(app, ["queue", "list", "--cluster", "test-cluster"])
    diagnosed = runner.invoke(app, ["queue", "diagnose", "--cluster", "test-cluster"])
    cleanup = runner.invoke(
        app,
        [
            "queue",
            "cleanup-stale",
            "--cluster",
            "test-cluster",
            "--no-dry-run",
        ],
    )
    canceled = runner.invoke(app, ["queue", "cancel", job.job_id])

    assert listed.exit_code == 0
    assert diagnosed.exit_code == 0
    assert cleanup.exit_code == 0
    assert canceled.exit_code == 0
    assert json.loads(listed.output)["count"] == 1
    assert json.loads(diagnosed.output)["issues"][0]["code"] == "expired_lease"
    assert json.loads(cleanup.output)["recovered_count"] == 1
    assert json.loads(canceled.output)["scheduler_policy"] == "relay-only"


def test_cli_worker_status_reports_registered_capacity(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="test-cluster",
            hostname="node",
            pid=123,
            metadata={"concurrency": 3},
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["worker", "status", "--cluster", "test-cluster"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["worker_count"] == 1
    assert payload["configured_concurrency"] == 3


def test_cli_job_submit_can_request_exclusive_scheduler(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    _write_test_cluster(tmp_path, name="test-cluster")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "submit",
            "--cluster",
            "test-cluster",
            "--jarvis-yaml",
            str(yaml_path),
            "--exclusive",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).list_jobs()[0]
    assert isinstance(job.spec, JarvisRunSpec)
    assert "exclusive: true" in str(job.spec.pipeline_yaml)


def test_cli_job_submit_pipeline_creates_named_jarvis_job(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(
        app,
        [
            "job",
            "submit-pipeline",
            "--cluster",
            "ares",
            "--pipeline-name",
            "lammps_4node",
            "--idempotency-key",
            "named-pipeline",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).get_job(result.output.strip())
    assert isinstance(job.spec, JarvisRunSpec)
    assert job.spec.pipeline_name == "lammps_4node"
    assert job.spec.pipeline_yaml is None


def test_cli_creates_and_evaluates_monitor_rule(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-monitor",
        )
    )
    queue.append_event(job.job_id, "stdout.delta", "step 25", payload={"text": "step 25\n"})
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    add_result = runner.invoke(
        app,
        [
            "monitor",
            "add-regex",
            job.job_id,
            "--pattern",
            "step 25",
            "--event-type",
            "stdout.delta",
        ],
    )
    run_result = runner.invoke(app, ["monitor", "run-once"])

    assert add_result.exit_code == 0
    assert json.loads(add_result.output)["job_id"] == job.job_id
    assert run_result.exit_code == 0
    assert json.loads(run_result.output)[0]["action"] == "emit_event"


def test_cli_accepts_json_object_from_file(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    payload_path = tmp_path / "progress-payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "label": "iteration",
                "current_group": "step",
                "total": 100,
                "unit": "step",
            }
        ),
        encoding="utf-8-sig",
    )
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-json-file",
        )
    )
    queue.append_event(job.job_id, "stdout.delta", "step 25", payload={"text": "step 25\n"})
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    add_result = runner.invoke(
        app,
        [
            "monitor",
            "add-regex",
            job.job_id,
            "--pattern",
            r"step (?P<step>\d+)",
            "--action",
            "record_progress",
            "--event-type",
            "stdout.delta",
            "--action-payload-json",
            f"@{payload_path}",
        ],
    )
    run_result = runner.invoke(app, ["monitor", "run-once"])

    assert add_result.exit_code == 0
    assert run_result.exit_code == 0
    progress = ClioCoreQueue(core_dir).list_progress(job.job_id)
    assert progress[0].label == "iteration"
    assert progress[0].current == 25


def test_cli_rejects_invalid_json_object() -> None:
    result = CliRunner().invoke(
        app,
        [
            "monitor",
            "add-regex",
            "job_abc",
            "--pattern",
            "step",
            "--action-payload-json",
            "{bad}",
        ],
    )

    assert result.exit_code != 0
    assert "value must be valid JSON" in result.output
    assert "Traceback" not in result.output


def test_cli_record_progress_cannot_spoof_package_progress(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-record-progress",
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(
        app,
        [
            "job",
            "record-progress",
            job.job_id,
            "--metadata-json",
            '{"source":"jarvis_package","package_name":"builtin.lammps","run_id":"spoofed"}',
        ],
    )

    assert result.exit_code == 0
    progress = ClioCoreQueue(core_dir).list_progress(job.job_id)[0]
    assert progress.metadata["source"] == "external_cli"
    assert "package_name" not in progress.metadata
    assert "run_id" not in progress.metadata


def test_cli_tests_ssh_transport(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_probe(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["transport.protocol=ssh_forward", "transport.healthz=ok"]

    monkeypatch.setattr("clio_relay.cli.run_ssh_forward_http_probe", fake_probe)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-ssh-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19001",
            "--remote-api-port",
            "9001",
            "--session-id",
            "session-1",
            "--detach-remote",
        ],
    )

    assert result.exit_code == 0
    assert "transport.healthz=ok" in result.output
    assert calls[0]["cluster"] == "ares"
    assert calls[0]["local_bind_port"] == 19001
    assert calls[0]["remote_api_port"] == 9001
    assert calls[0]["session_id"] == "session-1"
    assert calls[0]["api_token"] == "api-token"
    assert calls[0]["detach_remote"] is True


def test_cli_session_lifecycle_commands(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    started: list[dict[str, object]] = []
    torn_down: list[dict[str, object]] = []

    def fake_start(**kwargs: object) -> list[str]:
        started.append(kwargs)
        return ["session_started=session-1"]

    def fake_status(**kwargs: object) -> dict[str, object]:
        return {"session_id": kwargs["session_id"], "running": True}

    def fake_teardown(**kwargs: object) -> list[str]:
        torn_down.append(kwargs)
        return ["api_stopped=123", "worker_stopped=clio-relay-worker-ares.service"]

    monkeypatch.setattr("clio_relay.cli.start_remote_session", fake_start)
    monkeypatch.setattr("clio_relay.cli.status_remote_session", fake_status)
    monkeypatch.setattr("clio_relay.cli.teardown_remote_session", fake_teardown)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")
    runner = CliRunner()

    start_result = runner.invoke(
        app,
        [
            "session",
            "start",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--remote-api-port",
            "9001",
            "--replace",
        ],
    )
    status_result = runner.invoke(
        app,
        ["session", "status", "--cluster", "ares", "--session-id", "session-1"],
    )
    teardown_result = runner.invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--stop-worker",
            "--keep-jobs",
        ],
    )

    assert start_result.exit_code == 0
    assert "session_started=session-1" in start_result.output
    assert started[0]["api_token"] == "api-token"
    assert started[0]["replace"] is True
    assert status_result.exit_code == 0
    assert json.loads(status_result.output)["running"] is True
    assert teardown_result.exit_code == 0
    assert torn_down[0]["stop_worker"] is True
    assert torn_down[0]["cluster"] == "ares"


def test_cli_session_teardown_defaults_to_keep_jobs(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="keep-job",
        )
    )
    torn_down: list[dict[str, object]] = []

    def fake_teardown(**kwargs: object) -> list[str]:
        torn_down.append(kwargs)
        return ["session_teardown=session-1"]

    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setattr("clio_relay.cli.teardown_remote_session", fake_teardown)

    result = CliRunner().invoke(
        app,
        ["session", "teardown", "--cluster", "ares", "--session-id", "session-1"],
        input="\n",
    )

    assert result.exit_code == 0
    assert "Cancel queued or running jobs for cluster ares? [y/N]:" in result.output
    assert ClioCoreQueue(tmp_path / "core").get_job(job.job_id).state == JobState.QUEUED
    assert torn_down[0]["stop_worker"] is False


def test_cli_session_teardown_can_cancel_active_jobs(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    queue = ClioCoreQueue(tmp_path / "core")
    active = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-active-job",
        )
    )
    other_cluster = queue.submit_job(
        RelayJob(
            cluster="other",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="keep-other-cluster-job",
        )
    )
    torn_down: list[dict[str, object]] = []

    def fake_teardown(**kwargs: object) -> list[str]:
        torn_down.append(kwargs)
        return ["session_teardown=session-1"]

    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setattr("clio_relay.cli.teardown_remote_session", fake_teardown)

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--cancel-jobs",
        ],
    )

    refreshed = ClioCoreQueue(tmp_path / "core")
    assert result.exit_code == 0
    assert f"job_canceled={active.job_id}" in result.output
    assert refreshed.get_job(active.job_id).state == JobState.CANCELED
    assert refreshed.get_job(other_cluster.job_id).state == JobState.QUEUED
    assert torn_down[0]["stop_worker"] is False


def test_cli_render_frpc_uses_configured_secret_env(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "env-frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "env-stcp-secret")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 0
    assert 'auth.token = "env-frp-token"' in result.output
    assert 'secretKey = "env-stcp-secret"' in result.output


def test_cli_render_frpc_uses_local_secret_file(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    secret_dir = tmp_path / ".clio-relay"
    secret_dir.mkdir(exist_ok=True)
    (secret_dir / "secrets.json").write_text(
        json.dumps(
            {
                "CLIO_RELAY_FRP_TOKEN": "file-frp-token",
                "CLIO_RELAY_STCP_SECRET": "file-stcp-secret",
            }
        ),
        encoding="utf-8-sig",
    )
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 0
    assert 'auth.token = "file-frp-token"' in result.output
    assert 'secretKey = "file-stcp-secret"' in result.output


def test_cli_secret_file_rejects_non_string_secret(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    secret_dir = tmp_path / ".clio-relay"
    secret_dir.mkdir()
    (secret_dir / "secrets.json").write_text(
        json.dumps({"CLIO_RELAY_FRP_TOKEN": 123}),
        encoding="utf-8",
    )
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["relay-host", "render-frps-config"],
    )

    assert result.exit_code == 1
    assert "non-empty string" in result.output


def test_cli_transport_reports_missing_configured_secret_env(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 1
    assert "CLIO_RELAY_FRP_TOKEN" in result.output


def test_cli_transport_reports_missing_frp_server_addr(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, frp_server_addr="")
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret-key")

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 1
    assert "frp server address is not configured" in result.output


def test_cli_direct_transport_is_strict_xtcp_by_default(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret-key")
    calls: list[dict[str, object]] = []

    def fake_direct_probe(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["direct_transport.result=xtcp"]

    monkeypatch.setattr("clio_relay.cli.run_frp_direct_http_probe", fake_direct_probe)

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-direct-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19000",
        ],
    )

    assert result.exit_code == 0
    assert calls[0]["allow_stcp_fallback"] is False


def test_cli_init_creates_empty_cluster_registry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))

    result = CliRunner().invoke(app, ["init"])

    assert result.exit_code == 0
    assert "clusters=" in result.output
    assert "ares" not in result.output
    registry = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json")
    assert registry.clusters == {}


def test_cli_cluster_add_writes_explicit_definition(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "delta",
            "--ssh-host",
            "delta-login",
            "--agent-adapter",
            "exec",
            "--agent-npm-package",
            "",
            "--agent-npm-bin",
            "clio",
            "--frp-server-addr",
            "relay.example.edu",
            "--frp-protocol",
            "tcp",
            "--frp-server-port",
            "7000",
        ],
    )

    assert result.exit_code == 0
    registry = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json")
    definition = registry.require("delta")
    assert definition.ssh_host == "delta-login"
    assert definition.agent_adapter == "exec"
    assert definition.agent_npm_package is None
    assert definition.agent_npm_bin == "clio"
    assert definition.frp_transport.server_addr == "relay.example.edu"
    assert definition.frp_transport.protocol == "tcp"
    assert definition.frp_transport.server_port == 7000
    assert definition.frp_transport.direct.enabled is False
    assert definition.frp_transport.direct.fallback_order == ["frp_stcp", "queue"]


def test_cli_cluster_add_persists_direct_transport_optimization(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "homelab",
            "--ssh-host",
            "homelab",
            "--direct-transport",
            "--direct-transport-mode",
            "xtcp",
            "--direct-transport-fallback",
            "xtcp,frp_stcp,queue",
        ],
    )

    assert result.exit_code == 0
    definition = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json").require("homelab")
    assert definition.frp_transport.direct.enabled is True
    assert definition.frp_transport.direct.mode == "xtcp"
    assert definition.frp_transport.direct.fallback_order == ["xtcp", "frp_stcp", "queue"]
    assert definition.frp_transport.server_addr == ""


def test_cli_cluster_add_rejects_direct_transport_without_queue_fallback(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "homelab",
            "--ssh-host",
            "homelab",
            "--direct-transport",
            "--direct-transport-fallback",
            "xtcp,frp_stcp",
        ],
    )

    assert result.exit_code != 0
    assert "fallback_order must end with queue" in result.output


def test_cli_cluster_install_app_uses_explicit_app_installer(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, name="delta")
    calls: list[tuple[str, str]] = []

    def fake_install_cluster_app_over_ssh(*, ssh_host: str, app_name: str) -> list[str]:
        calls.append((ssh_host, app_name))
        return ["lammps_bin=/opt/lammps/bin/lmp"]

    monkeypatch.setattr(cli, "install_cluster_app_over_ssh", fake_install_cluster_app_over_ssh)

    result = CliRunner().invoke(
        app,
        ["cluster", "install-app", "--cluster", "delta", "--app", "lammps"],
    )

    assert result.exit_code == 0
    assert calls == [("delta", "lammps")]
    assert "lammps_bin=/opt/lammps/bin/lmp" in result.output


def test_cli_mcp_call_preserves_arguments(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            "ares",
            "--server",
            "remote-server",
            "--server-arg",
            "--stdio",
            "--tool",
            "simulate",
            "--arguments-json",
            '{"steps": 100, "case": "lammps"}',
            "--timeout-seconds",
            "90",
            "--idempotency-key",
            "cli-mcp-call-args",
        ],
    )

    assert result.exit_code == 0
    job_id = result.output.strip()
    job = ClioCoreQueue(core_dir).get_job(job_id)
    assert job.kind == JobKind.MCP_CALL
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.server == "remote-server"
    assert job.spec.server_args == ["--stdio"]
    assert job.spec.arguments == {"steps": 100, "case": "lammps"}
    assert job.spec.timeout_seconds == 90


def test_cli_jarvis_mcp_call_uses_builtin_cluster_command(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-call",
            "--cluster",
            "ares",
            "--tool",
            "jm_list_repos",
            "--idempotency-key",
            "cli-jarvis-mcp",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).get_job(result.output.strip())
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.server == "jarvis-mcp"
    assert job.spec.server_args == ["--profile", "user"]
    assert job.spec.tool == "jm_list_repos"


def test_cli_mcp_call_reads_arguments_json_file(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    arguments_path = tmp_path / "arguments.json"
    arguments_path.write_text('\ufeff{"steps": 150, "sample": "ares-live"}', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            "ares",
            "--server",
            "remote-server",
            "--tool",
            "echo",
            "--arguments-json-file",
            str(arguments_path),
            "--idempotency-key",
            "cli-mcp-call-file-args",
        ],
    )

    assert result.exit_code == 0
    job_id = result.output.strip()
    job = ClioCoreQueue(core_dir).get_job(job_id)
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.arguments == {"steps": 150, "sample": "ares-live"}


def test_cli_remote_job_submit_stages_yaml_and_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="test-host",
                core_dir="/remote/core",
                spool_dir="/remote/spool",
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    (tmp_path / "input.in").write_text("run 150\n", encoding="utf-8")
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text(
        """
name: remote-submit
x_clio_relay:
  stage_files:
    - local_path: input.in
      remote_path: .local/share/clio-relay/live-tests/{run_id}/input.in
pkgs:
  - pkg_type: builtin.lammps
    script: $HOME/.local/share/clio-relay/live-tests/{run_id}/input.in
""".lstrip(),
        encoding="utf-8",
    )
    writes: dict[str, bytes] = {}
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes | None = None,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        if command[2].startswith("cat > "):
            writes[command[2].removeprefix("cat > ").strip("'")] = input or b""
            return subprocess.CompletedProcess(command, 0, b"", b"")
        if "clio-relay job submit" in command[2]:
            assert "CLIO_RELAY_CLI_MODE=local" in command[2]
            assert 'CLIO_RELAY_CORE_DIR="/remote/core"' in command[2]
            return subprocess.CompletedProcess(command, 0, b"job_remote\n", b"")
        return subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "submit",
            "--cluster",
            "ares",
            "--jarvis-yaml",
            str(yaml_path),
            "--idempotency-key",
            "desktop-submit",
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "job_remote"
    assert ClioCoreQueue(tmp_path / ".clio-relay" / "core").list_jobs() == []
    assert any(path.endswith("/input.in") for path in writes)
    staged_yaml = next(
        data.decode("utf-8") for path, data in writes.items() if path.endswith("/pipeline.yaml")
    )
    assert "x_clio_relay" not in staged_yaml
    assert ".local/share/clio-relay/live-tests/pipeline-" in staged_yaml
    assert any("clio-relay job submit" in command[2] for command in commands)


def test_cli_remote_wait_passthrough_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b'{"job_id":"job_remote"}\n', b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "wait",
            "job_remote",
            "--cluster",
            "ares",
            "--timeout-seconds",
            "1",
            "--poll-seconds",
            "0.1",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["job_id"] == "job_remote"
    assert len(commands) == 1
    assert "clio-relay job wait job_remote" in commands[0][2]


def test_cli_cluster_bootstrap_uses_package_source_root(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    package_root = tmp_path / "package-root"
    wheel = tmp_path / "clio_relay-0.0.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    captured: dict[str, object] = {}

    def fake_package_source_root() -> Path:
        return package_root

    def fake_bootstrap_cluster_over_ssh(**kwargs: object) -> list[str]:
        captured.update(kwargs)
        return ["bootstrapped"]

    monkeypatch.setattr(cli, "package_source_root", fake_package_source_root)
    monkeypatch.setattr(cli, "bootstrap_cluster_over_ssh", fake_bootstrap_cluster_over_ssh)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "bootstrap",
            "--cluster",
            "ares",
            "--relay-wheel",
            str(wheel),
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "bootstrapped"
    assert captured["ssh_host"] == "ares"
    assert captured["source_root"] == package_root
    assert captured["source_root"] != tmp_path
    assert captured["relay_wheel"] == wheel


def test_cli_remote_task_event_passthrough_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    metadata_json = tmp_path / "metadata.json"
    metadata_json.write_text('{"surface":"cli-file"}', encoding="utf-8")
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b'{"seq":1}\n', b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "record-task-event",
            "task_remote",
            "--cluster",
            "ares",
            "--event-type",
            "dataset_found",
            "--label",
            "dataset",
            "--summary",
            "Found staged dataset",
            "--status",
            "succeeded",
            "--path-ref",
            "/mnt/common/datasets/example_001",
            "--metadata-json-file",
            str(metadata_json),
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["seq"] == 1
    assert len(commands) == 1
    assert "CLIO_RELAY_CLI_MODE=local" in commands[0][2]
    assert "clio-relay job record-task-event task_remote" in commands[0][2]
    assert "--path-ref /mnt/common/datasets/example_001" in commands[0][2]
    assert "cli-file" in commands[0][2]


def test_cli_remote_gateway_passthrough_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    gateway_json = tmp_path / "gateway.json"
    gateway_json.write_text('{"strategy":"ssh_forward","remote_port":11111}', encoding="utf-8")
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b'{"session_id":"gateway_remote"}\n', b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    created = CliRunner().invoke(
        app,
        [
            "gateway",
            "create",
            "--cluster",
            "ares",
            "--name",
            "live-service-example",
            "--gateway-json-file",
            str(gateway_json),
        ],
    )
    updated = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            "gateway_remote",
            "--cluster",
            "ares",
            "--state",
            "ready",
            "--node",
            "ares-comp-01",
        ],
    )
    closed = CliRunner().invoke(app, ["gateway", "close", "gateway_remote", "--cluster", "ares"])

    assert created.exit_code == 0
    assert updated.exit_code == 0
    assert closed.exit_code == 0
    assert [json.loads(item.output)["session_id"] for item in [created, updated, closed]] == [
        "gateway_remote",
        "gateway_remote",
        "gateway_remote",
    ]
    assert "clio-relay gateway create" in commands[0][2]
    assert "remote_port" in commands[0][2]
    assert "clio-relay gateway update gateway_remote" in commands[1][2]
    assert "clio-relay gateway close gateway_remote" in commands[2][2]


def test_cli_gateway_update_closed_session_reports_clean_error(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    created = CliRunner().invoke(
        app,
        ["gateway", "create", "--cluster", "ares", "--name", "live-service-example"],
    )
    assert created.exit_code == 0
    session_id = json.loads(created.output)["session_id"]

    closed = CliRunner().invoke(app, ["gateway", "close", session_id])
    updated = CliRunner().invoke(app, ["gateway", "update", session_id, "--state", "ready"])

    assert closed.exit_code == 0
    assert updated.exit_code == 1
    assert "error: cannot reopen closed gateway session" in updated.stderr
    assert "Traceback" not in updated.output
    assert "Traceback" not in updated.stderr


def test_cli_remote_agent_run_preserves_posix_prompt_path(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b"job_agent\n", b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "agent",
            "run",
            "--cluster",
            "ares",
            "--prompt",
            "/home/user/prompt.md",
            "--mcp-config",
            "/home/user/mcp.toml",
            "--idempotency-key",
            "agent-posix-path",
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "job_agent"
    assert "--prompt /home/user/prompt.md" in commands[0][2]
    assert "--mcp-config /home/user/mcp.toml" in commands[0][2]
    assert "\\home\\user" not in commands[0][2]

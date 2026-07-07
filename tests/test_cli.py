from __future__ import annotations

import json
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import (
    ArtifactRef,
    JarvisRunSpec,
    JobKind,
    McpCallSpec,
    RelayJob,
    RelayTask,
)


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


def test_cli_render_frpc_uses_configured_secret_env(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
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
    secret_dir = tmp_path / ".clio-relay"
    secret_dir.mkdir()
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


def test_cli_mcp_call_preserves_arguments(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
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
            "simulate",
            "--arguments-json",
            '{"steps": 100, "case": "lammps"}',
            "--idempotency-key",
            "cli-mcp-call-args",
        ],
    )

    assert result.exit_code == 0
    job_id = result.output.strip()
    job = ClioCoreQueue(core_dir).get_job(job_id)
    assert job.kind == JobKind.MCP_CALL
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.arguments == {"steps": 100, "case": "lammps"}

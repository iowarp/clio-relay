"""Tests for the ``job`` durable-record command group (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the thirteen durable-record ``job_app`` commands' extraction into
``src/clio_relay/cli_job_records.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises. The four
lifecycle commands' (submit/submit-pipeline/wait/cancel) tests moved
separately, to ``tests/test_cli_job.py``.
``test_remote_owned_job_discovery_never_cancels_unrelated_session`` stays in
``tests/test_cli.py`` -- it fakes a ``job tasks`` remote call as one
intermediate step inside a broader session-teardown flow, not a test of
``job tasks`` itself.

``monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", ...)`` already
patched the real collaborator directly (the R8(i) idiom, unaffected by
which file calls it).

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on). It is reproduced here (the env-var half only,
the same precedent ``tests/test_cli_relay_host.py``'s own
``_default_cli_mode`` established) -- the trap
``tests/test_cli_worker.py``'s docstring documents hitting for real.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import ArtifactRef, JarvisRunSpec, JobKind, RelayJob, RelayTask
from clio_relay.relay_ops import MAX_ARTIFACT_CONTENT_BYTES
from tests.test_cli import _write_test_cluster


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only."""
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
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
    page = json.loads(result.output)
    assert page["artifacts"][0]["artifact_id"] == artifact.artifact_id
    assert page["artifacts"][0]["kind"] == "stdout"
    assert page["cursor"] == 1
    assert page["limit"] == 100
    assert page["next_cursor"] is None
    assert page["total"] == 1


def test_cli_read_artifact_prints_document_and_exits_zero_on_a_normal_read(
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
            idempotency_key="cli-read-artifact-ok",
        )
    )
    owned_root = tmp_path / "spool" / job.job_id
    owned_root.mkdir(parents=True)
    artifact_path = owned_root / "stdout.log"
    artifact_path.write_text("hello\n", encoding="utf-8")
    artifact = queue.append_artifact(
        ArtifactRef(job_id=job.job_id, uri=artifact_path.as_uri(), kind="stdout")
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "read-artifact", artifact.artifact_id])

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["artifact"]["artifact_id"] == artifact.artifact_id


def test_cli_read_artifact_over_budget_prints_the_refusal_and_exits_nonzero(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """F6 (#231 R6 review): ``clio job read-artifact`` used to exit 0 while
    printing a T2 refusal document (doc SS6.4) -- a script checking only the
    exit code would treat an over-budget read as success. It must exit 1.
    """
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-read-artifact-oversized",
        )
    )
    owned_root = tmp_path / "spool" / job.job_id
    owned_root.mkdir(parents=True)
    oversized_path = owned_root / "large.bin"
    with oversized_path.open("wb") as stream:
        stream.truncate(MAX_ARTIFACT_CONTENT_BYTES + 1)
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=oversized_path.as_uri(),
            kind="stdout",
            size_bytes=MAX_ARTIFACT_CONTENT_BYTES + 1,
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "read-artifact", artifact.artifact_id])

    assert result.exit_code == 1, result.output
    document = json.loads(result.output)
    assert document["result_available"] is False
    assert document["delivery"]["code"] == "artifact_content_too_large"


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
    page = json.loads(result.output)
    assert page["tasks"][0]["task_id"] == task.task_id
    assert page["tasks"][0]["name"] == "jarvis.execution"
    assert page["cursor"] == 1
    assert page["limit"] == 100
    assert page["next_cursor"] is None
    assert page["total"] == 1


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
            '{"source":"jarvis_package","package_name":"site.simulation","run_id":"spoofed"}',
        ],
    )

    assert result.exit_code == 0
    progress = ClioCoreQueue(core_dir).list_progress(job.job_id)[0]
    assert progress.metadata["source"] == "external_cli"
    assert "package_name" not in progress.metadata
    assert "run_id" not in progress.metadata


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
    assert '"$HOME/.local/bin/clio-relay" job record-task-event task_remote' in commands[0][2]
    assert "--path-ref /mnt/common/datasets/example_001" in commands[0][2]
    assert "cli-file" in commands[0][2]

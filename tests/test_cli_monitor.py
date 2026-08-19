"""Tests for the ``monitor`` command group (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the ``monitor_app`` commands' extraction into
``src/clio_relay/cli_monitor.py``, per ground rule 3 (§2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.
"""

from __future__ import annotations

import json
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import JarvisRunSpec, JobKind, RelayJob


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

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from pytest import MonkeyPatch

from clio_relay.cluster_config import ClusterDefinition, LiveTestConfig
from clio_relay.errors import ConfigurationError
from clio_relay.live_acceptance import LiveAcceptanceOptions, run_live_acceptance


def test_live_acceptance_requires_configured_workload() -> None:
    with pytest.raises(ConfigurationError, match="live-test requires"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            )
        )


def test_live_acceptance_runs_configured_pipeline_and_monitor(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    commands: list[list[str]] = []
    uploaded: list[bytes | None] = []

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        uploaded.append(input)
        if "cat >" in " ".join(command):
            return _completed(command, "")
        script = command[-1]
        if "mkdir -p" in script:
            return _completed(command, "")
        if "job submit" in script:
            return _completed(command, "job_abc\n")
        if "job wait" in script:
            return _completed(command, json.dumps({"job_id": "job_abc", "state": "succeeded"}))
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ]
                    }
                ),
            )
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"next_offset": 0}))
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                    ]
                ),
            )
        if "read-artifact" in script:
            return _completed(command, json.dumps({"encoding": "base64", "data": "aGVsbG8="}))
        if "monitor add-regex" in script:
            return _completed(command, json.dumps({"rule_id": "rule_abc"}))
        if "monitor run-once" in script:
            return _completed(command, json.dumps([{"action": "emit_event"}]))
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)

    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(
                name="test-cluster",
                ssh_host="test-host",
                live_test=LiveTestConfig(monitor_pattern="done"),
            ),
            jarvis_yaml=pipeline,
        ),
        runner=fake_runner,
    )

    assert "acceptance.job_state=succeeded" in lines
    assert "acceptance.artifact_read=ok" in lines
    assert "acceptance.monitor=ok" in lines
    assert "live acceptance passed" in lines
    assert pipeline.read_bytes() in uploaded
    assert any("job submit" in " ".join(command) for command in commands)


def test_live_acceptance_uses_fresh_idempotency_key_per_run(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    submitted_scripts: list[str] = []

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        script = command[-1]
        if "job submit" in script:
            submitted_scripts.append(script)
            return _completed(command, f"job_{len(submitted_scripts)}\n")
        if "job wait" in script:
            job_id = f"job_{len(submitted_scripts)}"
            return _completed(command, json.dumps({"job_id": job_id, "state": "succeeded"}))
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ]
                    }
                ),
            )
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"next_offset": 0}))
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                    ]
                ),
            )
        if "read-artifact" in script:
            return _completed(command, json.dumps({"encoding": "base64", "data": "aGVsbG8="}))
        return _completed(command, "")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)

    for _ in range(2):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
                jarvis_yaml=pipeline,
            ),
            runner=fake_runner,
        )

    assert len(submitted_scripts) == 2
    assert submitted_scripts[0] != submitted_scripts[1]
    assert "live-test:test-cluster:" in submitted_scripts[0]
    assert "live-test:test-cluster:" in submitted_scripts[1]


def _completed(command: list[str], stdout: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.CompletedProcess(command, 0, stdout=stdout.encode(), stderr=b"")

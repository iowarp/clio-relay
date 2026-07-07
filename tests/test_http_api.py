from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from fastapi.testclient import TestClient

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.http_api import create_app
from clio_relay.models import ArtifactRef, JarvisRunSpec, JobKind, MonitorRule, RelayJob


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

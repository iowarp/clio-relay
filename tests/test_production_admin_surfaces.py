from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from clio_relay.adapters.worker.cli import worker_app as app
from clio_relay.adapters.worker.http_api import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
    create_app,
)
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.config import RelaySettings


def _small_storage_settings(tmp_path: Path) -> RelaySettings:
    """Build real storage limits small enough for deterministic admission tests."""
    return RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        spool_max_log_bytes_per_stream=50,
        spool_max_log_bytes_per_job=100,
        storage_core_high_water_bytes=1_000_000,
        storage_spool_high_water_bytes=1_000_000,
        storage_total_high_water_bytes=2_000_000,
        storage_minimum_free_bytes=0,
        storage_max_job_reservation_bytes=1_000,
        storage_max_scan_entries=10_000,
        storage_max_scan_depth=16,
        storage_max_scan_accounted_bytes=2_000_000,
        storage_max_ledger_bytes=1_000_000,
        storage_max_reservations=1,
        storage_job_core_allowance_bytes=20,
        storage_job_result_allowance_bytes=30,
        storage_runtime_check_interval_seconds=0.5,
    )


def _configure_storage_environment(
    monkeypatch: pytest.MonkeyPatch,
    settings: RelaySettings,
) -> None:
    values = {
        "CLIO_RELAY_CORE_DIR": settings.core_dir,
        "CLIO_RELAY_SPOOL_DIR": settings.spool_dir,
        "CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_STREAM": settings.spool_max_log_bytes_per_stream,
        "CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_JOB": settings.spool_max_log_bytes_per_job,
        "CLIO_RELAY_STORAGE_CORE_HIGH_WATER_BYTES": settings.storage_core_high_water_bytes,
        "CLIO_RELAY_STORAGE_SPOOL_HIGH_WATER_BYTES": settings.storage_spool_high_water_bytes,
        "CLIO_RELAY_STORAGE_TOTAL_HIGH_WATER_BYTES": settings.storage_total_high_water_bytes,
        "CLIO_RELAY_STORAGE_MINIMUM_FREE_BYTES": settings.storage_minimum_free_bytes,
        "CLIO_RELAY_STORAGE_MAX_JOB_RESERVATION_BYTES": (
            settings.storage_max_job_reservation_bytes
        ),
        "CLIO_RELAY_STORAGE_MAX_SCAN_ENTRIES": settings.storage_max_scan_entries,
        "CLIO_RELAY_STORAGE_MAX_SCAN_DEPTH": settings.storage_max_scan_depth,
        "CLIO_RELAY_STORAGE_MAX_SCAN_ACCOUNTED_BYTES": (settings.storage_max_scan_accounted_bytes),
        "CLIO_RELAY_STORAGE_MAX_LEDGER_BYTES": settings.storage_max_ledger_bytes,
        "CLIO_RELAY_STORAGE_MAX_RESERVATIONS": settings.storage_max_reservations,
        "CLIO_RELAY_STORAGE_JOB_CORE_ALLOWANCE_BYTES": (settings.storage_job_core_allowance_bytes),
        "CLIO_RELAY_STORAGE_JOB_RESULT_ALLOWANCE_BYTES": (
            settings.storage_job_result_allowance_bytes
        ),
        "CLIO_RELAY_STORAGE_RUNTIME_CHECK_INTERVAL_SECONDS": (
            settings.storage_runtime_check_interval_seconds
        ),
        "CLIO_RELAY_CLI_MODE": "local",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, str(value))


def test_http_storage_status_and_507_decision_are_machine_readable(tmp_path: Path) -> None:
    settings = _small_storage_settings(tmp_path)
    client = cast(Any, TestClient(create_app(settings)))

    status = client.get("/storage/status")
    first = client.post(
        "/jobs/jarvis",
        headers={
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
        json={
            "cluster": "configured-target",
            "pipeline_yaml": "name: first\npkgs: []\n",
            "idempotency_key": "http-storage-first",
        },
    )
    denied = client.post(
        "/jobs/jarvis",
        headers={
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
        json={
            "cluster": "configured-target",
            "pipeline_yaml": "name: second\npkgs: []\n",
            "idempotency_key": "http-storage-second",
        },
    )

    assert status.status_code == 200
    assert status.json()["schema"] == "clio-relay.storage-runtime-status.v1"
    assert first.status_code == 200
    assert denied.status_code == 507
    assert denied.json()["schema_version"] == "clio-relay.error.v1"
    assert denied.json()["reason"] == "storage_admission_refused"
    decision = denied.json()["storage_decision"]
    assert decision["schema"] == "clio-relay.storage-decision.v1"
    assert decision["allowed"] is False
    assert decision["reason"] == "ledger_capacity"


def test_cli_storage_admission_error_is_stable_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _small_storage_settings(tmp_path)
    _configure_storage_environment(monkeypatch, settings)
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "configured-target": ClusterDefinition(
                name="configured-target",
                ssh_host="localhost",
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: storage\npkgs: []\n", encoding="utf-8")
    runner = CliRunner()

    status = runner.invoke(app, ["storage", "status"])
    first = runner.invoke(
        app,
        [
            "job",
            "submit",
            "--cluster",
            "configured-target",
            "--jarvis-yaml",
            str(pipeline),
            "--idempotency-key",
            "cli-storage-first",
        ],
    )
    denied = runner.invoke(
        app,
        [
            "job",
            "submit",
            "--cluster",
            "configured-target",
            "--jarvis-yaml",
            str(pipeline),
            "--idempotency-key",
            "cli-storage-second",
        ],
    )

    assert status.exit_code == 0
    assert json.loads(status.output)["schema"] == "clio-relay.storage-runtime-status.v1"
    assert first.exit_code == 0
    assert denied.exit_code == 1
    envelope = json.loads(denied.output)
    assert envelope["error"] == "storage_admission_denied"
    assert envelope["storage_decision"]["reason"] == "ledger_capacity"

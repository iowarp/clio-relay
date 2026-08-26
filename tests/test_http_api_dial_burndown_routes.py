"""Route tests for the new owned-session-channel surfaces (iowarp/clio-
relay#179 dial burn-down): ``POST /session/quiesce-intake``, ``GET
/session/admission-status``, ``GET|POST /scheduler/...``, and ``GET
/worker-info``/``/target-info``. Follows ``tests/test_http_api.py``'s own
``TestClient(create_app(settings), headers={...})`` pattern.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

import clio_relay.http_api_routes_worker_probe as http_api_routes_worker_probe
from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.http_api import create_app
from clio_relay.job_identity import OWNER_SESSION_ID_HEADER, SESSION_GENERATION_ID_HEADER


def _bind_owned_session_cluster_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    definition: ClusterDefinition | None = None,
) -> ClusterDefinition:
    bound_definition = definition or ClusterDefinition(name="test-cluster", ssh_host="test-cluster")
    registry_path = tmp_path / "session-authority" / "clusters.json"
    ClusterRegistry(clusters={bound_definition.name: bound_definition}).save(registry_path)
    payload = registry_path.read_bytes()
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_SESSION_REGISTRY_SHA256", hashlib.sha256(payload).hexdigest())
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_ROUTE_REVISION", cluster_route_revision(bound_definition)
    )
    return bound_definition


def test_quiesce_intake_and_admission_status_over_owned_channel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )

    before = client.get("/session/admission-status")
    assert before.status_code == 200
    assert before.json()["owner_session_id"] == "desktop-session-1"
    assert before.json()["open"] is True
    assert before.json()["closing"] is False

    quiesced = client.post(
        "/session/quiesce-intake",
        json={
            "cleanup_operation_id": "cleanup_test1",
            "stop_worker": True,
            "cancel_jobs": True,
            "cancel_scheduler_jobs": False,
        },
    )
    assert quiesced.status_code == 200
    body = quiesced.json()
    assert body["session_id"] == "desktop-session-1"
    assert body["session_generation_id"] == "generation-1"
    assert body["intake"] == "quiesced"
    assert body["cleanup_intent"]["operation_id"] == "cleanup_test1"
    assert body["cleanup_intent"]["stop_worker"] is True
    assert body["cleanup_intent"]["cancel_jobs"] is True
    assert body["cleanup_intent"]["cancel_scheduler_jobs"] is False

    after = client.get("/session/admission-status")
    assert after.status_code == 200
    assert after.json()["closing"] is True
    assert after.json()["cleanup_intent"]["operation_id"] == "cleanup_test1"


def test_quiesce_intake_and_admission_status_require_an_owned_session(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))

    quiesce_response = client.post(
        "/session/quiesce-intake",
        json={
            "cleanup_operation_id": "cleanup_test2",
            "stop_worker": False,
            "cancel_jobs": False,
            "cancel_scheduler_jobs": False,
        },
    )
    status_response = client.get("/session/admission-status")

    assert quiesce_response.status_code == 404
    assert quiesce_response.json()["reason"] == "session_intake_quiescence_unavailable"
    assert status_response.status_code == 404
    assert status_response.json()["reason"] == "session_admission_status_unavailable"


def test_scheduler_status_status_batch_and_cancel_routes(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core", spool_dir=tmp_path / "spool", api_token="api-token"
    )
    client = cast(
        Any, TestClient(create_app(settings), headers={"Authorization": "Bearer api-token"})
    )

    status = client.get(
        "/scheduler/jobs/12345/status",
        params={"provider": "external"},
    )
    assert status.status_code == 200
    assert status.json()["scheduler"] == "external"
    assert status.json()["scheduler_job_id"] == "12345"
    assert status.json()["phase"] == "unknown"

    batch = client.post(
        "/scheduler/status-batch",
        json={"provider": "external", "scheduler_job_ids": ["12345", "67890"]},
    )
    assert batch.status_code == 200
    batch_body = batch.json()
    assert batch_body["schema_version"] == "clio-relay.scheduler-status-batch.v1"
    assert batch_body["scheduler"] == "external"
    assert {entry["scheduler_job_id"] for entry in batch_body["statuses"]} == {"12345", "67890"}

    duplicate_batch = client.post(
        "/scheduler/status-batch",
        json={"provider": "external", "scheduler_job_ids": ["12345", "12345"]},
    )
    assert duplicate_batch.status_code == 422
    assert duplicate_batch.json()["reason"] == "queue_query_refused"

    cancel = client.post("/scheduler/jobs/12345/cancel", json={"provider": "external"})
    assert cancel.status_code == 200
    cancel_body = cancel.json()
    assert cancel_body["scheduler"] == "external"
    assert cancel_body["scheduler_job_id"] == "12345"
    assert cancel_body["cancel_requested"] is True
    # ExternalSchedulerProvider.cancel() always refuses (returncode 2): the
    # route reports that refusal honestly rather than forcing "accepted".
    assert cancel_body["accepted"] is False


def test_scheduler_routes_reject_an_unknown_provider(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core", spool_dir=tmp_path / "spool", api_token="api-token"
    )
    client = cast(
        Any, TestClient(create_app(settings), headers={"Authorization": "Bearer api-token"})
    )

    response = client.get(
        "/scheduler/jobs/12345/status",
        params={"provider": "not-a-real-provider"},
    )

    assert response.status_code == 400
    assert response.json()["reason"] == "configuration_error"


def test_worker_info_route_passes_through_worker_runtime_info(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core", spool_dir=tmp_path / "spool", api_token="api-token"
    )
    client = cast(
        Any, TestClient(create_app(settings), headers={"Authorization": "Bearer api-token"})
    )
    captured: dict[str, object] = {}

    def fake_worker_runtime_info(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"schema_version": "clio-relay.worker-runtime-info.v1", "ready": True}

    monkeypatch.setattr(
        http_api_routes_worker_probe, "worker_runtime_info", fake_worker_runtime_info
    )

    response = client.get(
        "/worker-info",
        params={"cluster": "test-cluster", "freshness_seconds": 30, "readiness_only": True},
    )

    assert response.status_code == 200
    assert response.json() == {"schema_version": "clio-relay.worker-runtime-info.v1", "ready": True}
    assert captured["cluster"] == "test-cluster"
    assert captured["freshness_seconds"] == 30
    assert captured["readiness_only"] is True


def _fake_physical_site_marker_sha256(_path: Path) -> str:
    return "f" * 64


def test_target_info_route_reports_hostname_and_scheduler_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core", spool_dir=tmp_path / "spool", api_token="api-token"
    )
    client = cast(
        Any, TestClient(create_app(settings), headers={"Authorization": "Bearer api-token"})
    )
    monkeypatch.setattr(
        http_api_routes_worker_probe,
        "_physical_site_marker_sha256",
        _fake_physical_site_marker_sha256,
    )

    response = client.get("/target-info", params={"scheduler_provider": "external"})

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == "clio-relay.cluster-target-info.v1"
    assert body["site_marker_sha256"] == "f" * 64
    assert body["scheduler_provider"] == "external"
    assert body["scheduler_cluster_name"] is None
    assert isinstance(body["hostname"], str) and body["hostname"]

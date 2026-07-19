from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

import clio_relay.core_queue as core_queue_module
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.http_api import JarvisMcpCallSubmitRequest, create_app
from clio_relay.models import (
    McpCallSpec,
    RelayJob,
    deterministic_jarvis_execution_id,
)

_SERVER_ARTIFACT_DIGEST = "a" * 64
_JARVIS_CD_LOCK_BINDING = {
    "schema_version": "clio-relay.jarvis-cd-lock-expectation.v1",
    "version": "1.3.16",
    "url": "https://example.invalid/jarvis-cd-1.3.16-py3-none-any.whl",
    "sha256": "b" * 64,
}


def _pinned_jarvis_run(
    *,
    idempotency_key: str,
    arguments: dict[str, object] | None = None,
    job_id: str | None = None,
) -> RelayJob:
    values: dict[str, object] = {
        "cluster": "test-cluster",
        "kind": "mcp_call",
        "spec": McpCallSpec(
            server="clio-kit",
            expected_server_artifact_digest=_SERVER_ARTIFACT_DIGEST,
            expected_jarvis_cd_lock_binding=_JARVIS_CD_LOCK_BINDING,
            tool="jarvis_run",
            arguments=arguments or {"pipeline_id": "science-pipeline"},
        ),
        "idempotency_key": idempotency_key,
    }
    if job_id is not None:
        values["job_id"] = job_id
    return RelayJob.model_validate(values)


def _mcp_spec(job: RelayJob) -> McpCallSpec:
    assert isinstance(job.spec, McpCallSpec)
    return job.spec


def test_legacy_pinned_jarvis_run_with_wait_deserializes_unchanged() -> None:
    legacy = _pinned_jarvis_run(
        idempotency_key="legacy-wait-record",
        arguments={"pipeline_id": "science-pipeline", "wait": True},
    )

    restored = RelayJob.model_validate_json(legacy.model_dump_json())

    assert restored == legacy
    assert _mcp_spec(restored).arguments == {
        "pipeline_id": "science-pipeline",
        "wait": True,
    }


def test_queue_submission_rejects_caller_owned_wait(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    submission = _pinned_jarvis_run(
        idempotency_key="reject-caller-wait",
        arguments={"pipeline_id": "science-pipeline", "wait": False},
    )

    with pytest.raises(ValueError, match="does not accept internal wait"):
        queue.submit_job(submission)


def test_queue_persists_deterministic_execution_id(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    submission = _pinned_jarvis_run(idempotency_key="deterministic-execution")
    expected_execution_id = deterministic_jarvis_execution_id(
        cluster=submission.cluster,
        idempotency_key=submission.idempotency_key,
        job_id=submission.job_id,
    )

    accepted = queue.submit_job(submission)
    persisted = queue.get_job(accepted.job_id)

    assert "execution_id" not in _mcp_spec(submission).arguments
    assert _mcp_spec(accepted).arguments["execution_id"] == expected_execution_id
    assert _mcp_spec(persisted).arguments["execution_id"] == expected_execution_id


def test_queue_preserves_matching_relay_owned_execution_id(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    idempotency_key = "preserve-execution"
    job_id = "job_11111111111111111111111111111111"
    supplied_execution_id = deterministic_jarvis_execution_id(
        cluster="test-cluster",
        idempotency_key=idempotency_key,
        job_id=job_id,
    )
    submission = _pinned_jarvis_run(
        idempotency_key=idempotency_key,
        job_id=job_id,
        arguments={
            "pipeline_id": "science-pipeline",
            "execution_id": supplied_execution_id,
        },
    )

    accepted = queue.submit_job(submission)
    persisted = queue.get_job(accepted.job_id)

    assert _mcp_spec(accepted).arguments["execution_id"] == supplied_execution_id
    assert _mcp_spec(persisted).arguments["execution_id"] == supplied_execution_id


def test_queue_rejects_execution_id_not_owned_by_submission(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    submission = _pinned_jarvis_run(
        idempotency_key="reject-adopted-execution",
        arguments={
            "pipeline_id": "science-pipeline",
            "execution_id": "jarvis_preexisting-execution",
        },
    )

    with pytest.raises(ValueError, match="must match the relay-owned"):
        queue.submit_job(submission)


def test_execution_identity_includes_server_owned_job_id() -> None:
    first = deterministic_jarvis_execution_id(
        cluster="test-cluster",
        idempotency_key="client-visible-key",
        job_id="job_33333333333333333333333333333333",
    )
    second = deterministic_jarvis_execution_id(
        cluster="test-cluster",
        idempotency_key="client-visible-key",
        job_id="job_44444444444444444444444444444444",
    )

    assert first != second


def test_idempotent_resolution_prepares_missing_execution_id(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    original = _pinned_jarvis_run(idempotency_key="handle-first-replay")
    accepted = queue.submit_job(original)
    retry = _pinned_jarvis_run(idempotency_key=original.idempotency_key)

    resolution = queue.resolve_idempotent_submission(retry)

    assert resolution.state == "existing"
    assert resolution.canonical_job_id == accepted.job_id
    assert resolution.existing_job == accepted
    assert resolution.existing_job is not None
    assert _mcp_spec(resolution.existing_job).arguments["execution_id"] == (
        deterministic_jarvis_execution_id(
            cluster=original.cluster,
            idempotency_key=original.idempotency_key,
            job_id=accepted.job_id,
        )
    )


def test_idempotent_submit_reuses_canonical_job_execution_identity(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    first = queue.submit_job(_pinned_jarvis_run(idempotency_key="canonical-handle-replay"))
    retry_request = _pinned_jarvis_run(idempotency_key="canonical-handle-replay")

    replay = queue.submit_job(retry_request)

    assert retry_request.job_id != first.job_id
    assert replay == first


def test_idempotent_retry_replays_pre_handle_jarvis_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Relay-generated handles must not break durable pre-upgrade retries."""
    queue = ClioCoreQueue(tmp_path)
    original = _pinned_jarvis_run(idempotency_key="legacy-handle-replay")

    def preserve_legacy_submission(job: RelayJob) -> RelayJob:
        return job

    with monkeypatch.context() as legacy_release:
        legacy_release.setattr(
            core_queue_module,
            "prepare_owned_jarvis_run_submission",
            preserve_legacy_submission,
        )
        accepted = queue.submit_job(original)
    assert "execution_id" not in _mcp_spec(accepted).arguments

    retry = _pinned_jarvis_run(idempotency_key=original.idempotency_key)
    resolution = queue.resolve_idempotent_submission(retry)
    replay = queue.submit_job(retry)

    assert resolution.state == "existing"
    assert resolution.existing_job == accepted
    assert replay == accepted


def test_raw_http_submission_cannot_select_jarvis_handle_identity(
    tmp_path: Path,
) -> None:
    """The raw route rejects MCP jobs before caller identity reaches intake."""
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        storage_minimum_free_bytes=0,
    )
    queue = ClioCoreQueue(settings.core_dir)
    client = cast(Any, TestClient(create_app(settings)))
    caller_job_id = "job_55555555555555555555555555555555"
    submission = _pinned_jarvis_run(
        idempotency_key="raw-http-server-owned-identity",
        job_id=caller_job_id,
    )

    response = client.post("/jobs", json=submission.model_dump(mode="json"))

    assert response.status_code == 422
    assert response.json()["detail"] == (
        "MCP jobs must use /jobs/mcp-call or /jobs/jarvis-mcp-call"
    )
    assert queue.list_jobs() == []


def test_typed_http_jarvis_run_retry_replays_server_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fresh HTTP entropy must not weaken typed-route idempotent retries."""
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        storage_minimum_free_bytes=0,
    )
    queue = ClioCoreQueue(settings.core_dir)

    def artifact_binding(_cluster: str) -> str:
        return _SERVER_ARTIFACT_DIGEST

    monkeypatch.setattr(
        "clio_relay.http_api.jarvis_mcp_artifact_binding",
        artifact_binding,
    )
    client = cast(Any, TestClient(create_app(settings)))
    payload = {
        "cluster": "test-cluster",
        "tool": "jarvis_run",
        "arguments": {"pipeline_id": "science-pipeline"},
        "expected_server_artifact_digest": _SERVER_ARTIFACT_DIGEST,
        "idempotency_key": "typed-http-handle-replay",
    }

    first = client.post("/jobs/jarvis-mcp-call", json=payload)
    retry = client.post("/jobs/jarvis-mcp-call", json=payload)

    assert first.status_code == 200
    assert retry.status_code == 200
    assert retry.json() == first.json()
    accepted = queue.get_job(first.json()["job_id"])
    assert _mcp_spec(accepted).arguments["execution_id"] == (
        deterministic_jarvis_execution_id(
            cluster=accepted.cluster,
            idempotency_key=accepted.idempotency_key,
            job_id=accepted.job_id,
        )
    )


def test_typed_http_jarvis_run_rejects_caller_execution_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caller-selected execution identities fail explicitly rather than as HTTP 500."""
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        storage_minimum_free_bytes=0,
    )

    def artifact_binding(_cluster: str) -> str:
        return _SERVER_ARTIFACT_DIGEST

    monkeypatch.setattr(
        "clio_relay.http_api.jarvis_mcp_artifact_binding",
        artifact_binding,
    )
    client = cast(Any, TestClient(create_app(settings)))

    response = client.post(
        "/jobs/jarvis-mcp-call",
        json={
            "cluster": "test-cluster",
            "tool": "jarvis_run",
            "arguments": {
                "pipeline_id": "science-pipeline",
                "execution_id": "jarvis_caller_selected",
            },
            "expected_server_artifact_digest": _SERVER_ARTIFACT_DIGEST,
            "idempotency_key": "typed-http-reject-caller-handle",
        },
    )

    assert response.status_code == 422
    assert "must match the relay-owned" in response.json()["detail"]


def test_http_jarvis_run_request_rejects_wait() -> None:
    with pytest.raises(ValidationError, match="does not accept internal wait"):
        JarvisMcpCallSubmitRequest(
            cluster="test-cluster",
            tool="jarvis_run",
            arguments={"pipeline_id": "science-pipeline", "wait": True},
            expected_server_artifact_digest=_SERVER_ARTIFACT_DIGEST,
            idempotency_key="http-reject-wait",
        )

"""Focused tests for private owned-session input artifact ingestion."""

from __future__ import annotations

import asyncio
import base64
import hashlib
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY, ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.http_api import InputArtifactBodyLimitMiddleware, create_app
from clio_relay.models import (
    INPUT_INGEST_POLICY_METADATA_KEY,
    EndpointRegistration,
    EndpointRole,
    InputArtifactIngestPolicy,
    InputArtifactSpec,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    deterministic_input_artifact_id,
    new_id,
    utc_now,
)
from clio_relay.queue_management import diagnose_job
from clio_relay.session_api import OWNER_SESSION_ID_HEADER, SESSION_GENERATION_ID_HEADER
from clio_relay.spool import JobSpool
from clio_relay.storage_runtime import storage_managed_queue


def _owned_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    max_bytes: int = 1_048_576,
    max_count: int = 16,
    max_total_bytes: int | None = None,
) -> RelaySettings:
    """Create one exact process-bound owner-session API configuration."""
    definition = ClusterDefinition(name="test-cluster", ssh_host="test-cluster")
    registry_path = tmp_path / "session-authority" / "clusters.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    payload = registry_path.read_bytes()
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, str(registry_path))
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_REGISTRY_SHA256",
        hashlib.sha256(payload).hexdigest(),
    )
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_ROUTE_REVISION",
        cluster_route_revision(definition),
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
        input_file_max_bytes=max_bytes,
        input_total_max_bytes=max_total_bytes or max(max_bytes, 4),
        input_file_max_count=max_count,
    )
    queue = ClioCoreQueue(settings.core_dir)
    selected = queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    assert selected == "generation-1"
    return settings


def _headers() -> dict[str, str]:
    """Return exact API and owner-generation authentication headers."""
    return {
        "Authorization": "Bearer session-api-token",
        OWNER_SESSION_ID_HEADER: "desktop-session-1",
        SESSION_GENERATION_ID_HEADER: "generation-1",
    }


def _input_metadata(
    *,
    max_count: int = 16,
    max_total_bytes: int = 4_194_304,
) -> dict[str, object]:
    """Return exact owner identity plus a server-stamped ingest quota."""
    return {
        "owner": "clio-relay",
        "owner_session_id": "desktop-session-1",
        "owner_session_generation_id": "generation-1",
        INPUT_INGEST_POLICY_METADATA_KEY: InputArtifactIngestPolicy(
            max_file_count=max_count,
            max_total_bytes=max_total_bytes,
        ).model_dump(mode="json"),
    }


def _request(payload: bytes, *, idempotency_key: str = "input-ingest-1") -> dict[str, object]:
    """Build one canonical private ingest request."""
    return {
        "schema_version": "clio-relay.input-artifact-ingest.v1",
        "cluster": "test-cluster",
        "logical_name": "in.lammps",
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "data_base64": base64.b64encode(payload).decode("ascii"),
        "idempotency_key": idempotency_key,
    }


def test_owned_input_ingest_is_terminal_content_pinned_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _owned_settings(tmp_path, monkeypatch)
    payload = b"units lj\nrun 50\n"

    with TestClient(create_app(settings), headers=_headers()) as raw_client:
        client = cast(Any, raw_client)
        first = client.post("/input-artifacts/ingest", json=_request(payload))
        repeated = client.post("/input-artifacts/ingest", json=_request(payload))
        openapi = client.get("/openapi.json")

    assert first.status_code == 200
    assert repeated.status_code == 200
    assert first.json() == repeated.json()
    document = first.json()
    job = document["job"]
    artifact = document["artifact"]
    assert job["kind"] == "input_ingest"
    assert job["state"] == "succeeded"
    assert job["leased_by"] is None
    assert job["metadata"]["owner_session_id"] == "desktop-session-1"
    assert job["metadata"]["owner_session_generation_id"] == "generation-1"
    assert artifact["artifact_id"] == deterministic_input_artifact_id(job["job_id"])
    assert artifact["job_id"] == job["job_id"]
    assert artifact["kind"] == "input"
    assert artifact["size_bytes"] == len(payload)
    assert artifact["sha256"] == hashlib.sha256(payload).hexdigest()
    assert artifact["metadata"]["logical_name"] == "in.lammps"
    assert (settings.spool_dir / job["job_id"] / "inputs" / "in.lammps").read_bytes() == payload
    assert "/input-artifacts/ingest" not in openapi.json()["paths"]

    queue = ClioCoreQueue(settings.core_dir)
    assert queue.get_job(job["job_id"]).state is JobState.SUCCEEDED
    assert queue.list_artifacts(job["job_id"])[0].artifact_id == artifact["artifact_id"]
    events, _ = queue.read_event_page(job["job_id"], limit=10)
    assert [event.event_type for event in events] == [
        "job.queued",
        "input_ingest.started",
        "artifact.created",
        "job.succeeded",
    ]


def test_owned_input_ingest_failure_releases_capacity_and_exact_retry_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _owned_settings(tmp_path, monkeypatch)
    payload = b"units lj\nrun 50\n"
    original_write = JobSpool.write_input_artifact
    calls = 0

    def fail_once(
        self: JobSpool,
        logical_name: str,
        content: bytes,
        *,
        size_bytes: int,
        sha256: str,
    ) -> Path:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("injected atomic input write failure")
        return original_write(
            self,
            logical_name,
            content,
            size_bytes=size_bytes,
            sha256=sha256,
        )

    monkeypatch.setattr(JobSpool, "write_input_artifact", fail_once)
    with TestClient(create_app(settings), headers=_headers()) as raw_client:
        client = cast(Any, raw_client)
        failed_response = client.post("/input-artifacts/ingest", json=_request(payload))

    assert failed_response.status_code == 409
    queue = ClioCoreQueue(settings.core_dir)
    jobs = queue.list_jobs()
    assert len(jobs) == 1
    assert jobs[0].state is JobState.FAILED
    assert "injected atomic input write failure" in (jobs[0].last_error or "")
    managed = storage_managed_queue(settings)
    storage_status = managed.storage_runtime.policy.status().status
    assert storage_status is not None
    assert storage_status.reservation_count == 0
    managed.close()

    with TestClient(create_app(settings), headers=_headers()) as raw_client:
        client = cast(Any, raw_client)
        retried = client.post("/input-artifacts/ingest", json=_request(payload))

    assert retried.status_code == 200
    assert retried.json()["job"]["job_id"] == jobs[0].job_id
    assert retried.json()["job"]["state"] == "succeeded"


def test_abandoned_input_ingests_release_capacity_and_retry_is_single_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _owned_settings(tmp_path, monkeypatch)
    queue = storage_managed_queue(settings)
    spec = InputArtifactSpec(
        logical_name="input.txt",
        size_bytes=0,
        sha256=hashlib.sha256(b"").hexdigest(),
    )
    queued = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.INPUT_INGEST,
            spec=spec,
            idempotency_key="abandoned-before-claim",
            metadata=_input_metadata(),
        )
    )
    running = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.INPUT_INGEST,
            spec=spec.model_copy(update={"logical_name": "second.txt"}),
            idempotency_key="abandoned-after-claim",
            metadata=_input_metadata(),
        )
    )
    first_attempt = new_id("ingest_attempt")
    started, changed = queue.begin_input_ingest(running.job_id, attempt_id=first_attempt)
    assert changed is True
    assert started.state is JobState.RUNNING
    with pytest.raises(QueueConflictError, match="active attempt"):
        queue.begin_input_ingest(
            running.job_id,
            attempt_id=new_id("ingest_attempt"),
        )

    recovered = queue.recover_abandoned_input_ingests(
        cluster="test-cluster",
        stale_before=utc_now(),
    )

    assert {job.job_id for job in recovered} == {queued.job_id, running.job_id}
    assert all(job.state is JobState.FAILED for job in recovered)
    assert queue.storage_runtime.policy.release(queued.job_id).reason.value == (
        "reservation_absent"
    )
    assert queue.storage_runtime.policy.release(running.job_id).reason.value == (
        "reservation_absent"
    )

    retry_attempt = new_id("ingest_attempt")
    retried, retried_changed = queue.begin_input_ingest(
        running.job_id,
        attempt_id=retry_attempt,
    )
    estimate = queue.storage_runtime.estimate(retried)
    reservation = queue.storage_runtime.policy.verify_reservation(
        retried.job_id,
        core_bytes=estimate.core_bytes,
        spool_bytes=estimate.spool_bytes,
    )
    assert retried_changed is True
    assert retried.state is JobState.RUNNING
    assert reservation.allowed is True
    failed, failed_changed = queue.fail_input_ingest(
        retried.job_id,
        attempt_id=retry_attempt,
        error="test cleanup",
    )
    assert failed_changed is True
    assert failed.state is JobState.FAILED
    queue.close()


def test_recovered_ingest_with_durable_artifact_still_consumes_quota(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _owned_settings(
        tmp_path,
        monkeypatch,
        max_bytes=4,
        max_count=1,
        max_total_bytes=4,
    )
    queue = storage_managed_queue(settings)
    payload = b"data"
    policy = InputArtifactIngestPolicy(max_file_count=1, max_total_bytes=4)
    spec = InputArtifactSpec(
        logical_name="input.txt",
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    submitted = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.INPUT_INGEST,
            spec=spec,
            idempotency_key="crash-after-reconcile",
            metadata=_input_metadata(max_count=1, max_total_bytes=4),
        )
    )
    first_attempt = new_id("ingest_attempt")
    running, _changed = queue.begin_input_ingest(
        submitted.job_id,
        attempt_id=first_attempt,
        policy=policy,
    )
    spool = JobSpool(
        settings.spool_dir,
        running,
        max_log_bytes_per_stream=settings.spool_max_log_bytes_per_stream,
        max_log_bytes_per_job=settings.spool_max_log_bytes_per_job,
    )
    path = spool.write_input_artifact(
        spec.logical_name,
        payload,
        size_bytes=spec.size_bytes,
        sha256=spec.sha256,
    )
    candidate = spool.artifact_for(path, kind="input").model_copy(
        update={
            "artifact_id": deterministic_input_artifact_id(running.job_id),
            "created_at": running.created_at,
            "metadata": {
                "schema_version": spec.schema_version,
                "logical_name": spec.logical_name,
            },
        }
    )
    artifact = queue.reconcile_input_artifact(candidate, attempt_id=first_attempt)
    recovered = queue.recover_abandoned_input_ingests(
        cluster="test-cluster",
        stale_before=utc_now(),
    )

    assert [job.job_id for job in recovered] == [running.job_id]
    assert queue.get_artifact(artifact.artifact_id).sha256 == spec.sha256
    with pytest.raises(QueueConflictError, match="file_count_limit_reached"):
        queue.submit_job(
            RelayJob(
                cluster="test-cluster",
                kind=JobKind.INPUT_INGEST,
                spec=spec.model_copy(update={"logical_name": "other.txt"}),
                idempotency_key="quota-after-reconciled-crash",
                metadata=_input_metadata(max_count=1, max_total_bytes=4),
            )
        )

    retry_attempt = new_id("ingest_attempt")
    retried, retried_changed = queue.begin_input_ingest(
        running.job_id,
        attempt_id=retry_attempt,
        policy=policy,
    )
    completed, completed_changed = queue.complete_input_ingest(
        retried.job_id,
        attempt_id=retry_attempt,
    )
    assert retried_changed is True
    assert completed_changed is True
    assert completed.state is JobState.SUCCEEDED
    queue.close()


def test_failed_ingest_retry_enforces_and_audits_current_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _owned_settings(tmp_path, monkeypatch, max_bytes=4, max_total_bytes=10)
    queue = storage_managed_queue(settings)
    original_policy = InputArtifactIngestPolicy(max_file_count=16, max_total_bytes=10)
    spec = InputArtifactSpec(
        logical_name="input.txt",
        size_bytes=4,
        sha256=hashlib.sha256(b"data").hexdigest(),
    )
    submitted = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.INPUT_INGEST,
            spec=spec,
            idempotency_key="policy-drift-retry",
            metadata=_input_metadata(max_total_bytes=10),
        )
    )
    attempt = new_id("ingest_attempt")
    queue.begin_input_ingest(submitted.job_id, attempt_id=attempt, policy=original_policy)
    queue.fail_input_ingest(submitted.job_id, attempt_id=attempt, error="retry me")

    with pytest.raises(QueueConflictError, match="total_bytes_limit_reached"):
        queue.begin_input_ingest(
            submitted.job_id,
            attempt_id=new_id("ingest_attempt"),
            policy=InputArtifactIngestPolicy(max_file_count=16, max_total_bytes=2),
        )

    current_policy = InputArtifactIngestPolicy(max_file_count=8, max_total_bytes=8)
    retry_attempt = new_id("ingest_attempt")
    retried, changed = queue.begin_input_ingest(
        submitted.job_id,
        attempt_id=retry_attempt,
        policy=current_policy,
    )

    assert changed is True
    assert retried.metadata[INPUT_INGEST_POLICY_METADATA_KEY] == current_policy.model_dump(
        mode="json"
    )
    assert retried.metadata[INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY] == (
        original_policy.model_dump(mode="json")
    )
    queue.fail_input_ingest(retried.job_id, attempt_id=retry_attempt, error="test cleanup")
    queue.close()


def test_input_ingest_body_limit_rejects_chunked_payload_before_downstream() -> None:
    async def scenario() -> tuple[bool, list[dict[str, Any]]]:
        downstream_called = False
        sent: list[dict[str, Any]] = []
        incoming: list[dict[str, Any]] = [
            {"type": "http.request", "body": b"12345", "more_body": True},
            {"type": "http.request", "body": b"67890", "more_body": False},
        ]

        async def downstream(
            _scope: dict[str, Any],
            _receive: Any,
            _send: Any,
        ) -> None:
            nonlocal downstream_called
            downstream_called = True

        async def receive() -> dict[str, Any]:
            return incoming.pop(0)

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        middleware = InputArtifactBodyLimitMiddleware(
            cast(Any, downstream),
            max_body_bytes=8,
            api_token="session-api-token",
            owner_session_id="desktop-session-1",
            session_generation_id="generation-1",
        )
        await middleware(
            cast(
                Any,
                {
                    "type": "http",
                    "method": "POST",
                    "path": "/input-artifacts/ingest",
                    "headers": [
                        (b"authorization", b"Bearer session-api-token"),
                        (b"x-clio-relay-owner-session-id", b"desktop-session-1"),
                        (b"x-clio-relay-session-generation-id", b"generation-1"),
                    ],
                },
            ),
            cast(Any, receive),
            cast(Any, send),
        )
        return downstream_called, sent

    downstream_called, sent = asyncio.run(scenario())

    assert downstream_called is False
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 413


def test_input_ingest_body_limit_authenticates_before_receiving_body() -> None:
    async def scenario() -> tuple[bool, bool, list[dict[str, Any]]]:
        downstream_called = False
        receive_called = False
        sent: list[dict[str, Any]] = []

        async def downstream(
            _scope: dict[str, Any],
            _receive: Any,
            _send: Any,
        ) -> None:
            nonlocal downstream_called
            downstream_called = True

        async def receive() -> dict[str, Any]:
            nonlocal receive_called
            receive_called = True
            return {"type": "http.request", "body": b"x" * 1_000_000}

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        middleware = InputArtifactBodyLimitMiddleware(
            cast(Any, downstream),
            max_body_bytes=2_000_000,
            api_token="session-api-token",
            owner_session_id="desktop-session-1",
            session_generation_id="generation-1",
        )
        await middleware(
            cast(
                Any,
                {
                    "type": "http",
                    "method": "POST",
                    "path": "/input-artifacts/ingest",
                    "headers": [(b"authorization", b"Bearer wrong-token")],
                },
            ),
            cast(Any, receive),
            cast(Any, send),
        )
        return downstream_called, receive_called, sent

    downstream_called, receive_called, sent = asyncio.run(scenario())

    assert downstream_called is False
    assert receive_called is False
    assert sent[0]["status"] == 401


def test_owned_input_ingest_rejects_auth_drift_payload_drift_and_oversize(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _owned_settings(tmp_path, monkeypatch, max_bytes=4)
    request = _request(b"1234")

    with TestClient(create_app(settings)) as raw_client:
        client = cast(Any, raw_client)
        missing_generation = client.post(
            "/input-artifacts/ingest",
            headers={"Authorization": "Bearer session-api-token"},
            json=request,
        )
        wrong_generation = client.post(
            "/input-artifacts/ingest",
            headers={
                **_headers(),
                SESSION_GENERATION_ID_HEADER: "generation-2",
            },
            json=request,
        )
        accepted = client.post(
            "/input-artifacts/ingest",
            headers=_headers(),
            json=request,
        )
        changed = client.post(
            "/input-artifacts/ingest",
            headers=_headers(),
            json=_request(b"5678"),
        )
        oversize = client.post(
            "/input-artifacts/ingest",
            headers=_headers(),
            json=_request(b"12345", idempotency_key="oversize"),
        )
        invalid_name_request = _request(b"1234", idempotency_key="invalid-name")
        invalid_name_request["logical_name"] = "../in.lammps"
        invalid_name = client.post(
            "/input-artifacts/ingest",
            headers=_headers(),
            json=invalid_name_request,
        )

    assert missing_generation.status_code == 409
    assert wrong_generation.status_code == 409
    assert accepted.status_code == 200
    assert changed.status_code == 409
    assert "idempotency key" in changed.json()["detail"]
    assert oversize.status_code == 422
    assert "per-file limit" in oversize.json()["detail"]
    assert invalid_name.status_code == 422


def test_owner_generation_input_quotas_are_remote_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    count_settings = _owned_settings(
        tmp_path / "count",
        monkeypatch,
        max_bytes=4,
        max_count=1,
        max_total_bytes=8,
    )
    first_request = _request(b"1234", idempotency_key="quota-first")
    second_request = _request(b"12", idempotency_key="quota-second")
    with TestClient(create_app(count_settings), headers=_headers()) as raw_client:
        client = cast(Any, raw_client)
        first = client.post("/input-artifacts/ingest", json=first_request)
        replay = client.post("/input-artifacts/ingest", json=first_request)
        count_exceeded = client.post("/input-artifacts/ingest", json=second_request)

    assert first.status_code == 200
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert count_exceeded.status_code == 409
    assert "input_ingest_file_count_limit_reached" in count_exceeded.json()["detail"]

    byte_settings = _owned_settings(
        tmp_path / "bytes",
        monkeypatch,
        max_bytes=4,
        max_count=3,
        max_total_bytes=6,
    )
    with TestClient(create_app(byte_settings), headers=_headers()) as raw_client:
        client = cast(Any, raw_client)
        accepted = client.post(
            "/input-artifacts/ingest",
            json=_request(b"1234", idempotency_key="bytes-first"),
        )
        bytes_exceeded = client.post(
            "/input-artifacts/ingest",
            json=_request(b"567", idempotency_key="bytes-second"),
        )

    assert accepted.status_code == 200
    assert bytes_exceeded.status_code == 409
    assert "input_ingest_total_bytes_limit_reached" in bytes_exceeded.json()["detail"]


def test_input_ingest_jobs_are_never_worker_lease_candidates(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.INPUT_INGEST,
            spec=InputArtifactSpec(
                logical_name="input.txt",
                size_bytes=0,
                sha256=hashlib.sha256(b"").hexdigest(),
            ),
            idempotency_key="never-lease-input",
            metadata=_input_metadata(),
        )
    )

    assert queue.acquire_next_job("endpoint_worker", cluster="test-cluster") is None
    assert queue.acquire_job(job.job_id, "endpoint_worker", cluster="test-cluster") is None
    assert queue.get_job(job.job_id).state is JobState.QUEUED


def test_storage_managed_input_ingest_jobs_are_never_worker_leased(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _owned_settings(tmp_path, monkeypatch)
    queue = storage_managed_queue(settings)
    spec = InputArtifactSpec(
        logical_name="input.txt",
        size_bytes=0,
        sha256=hashlib.sha256(b"").hexdigest(),
    )
    metadata = _input_metadata()
    first = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.INPUT_INGEST,
            spec=spec,
            idempotency_key="managed-never-lease-input",
            metadata=metadata,
        )
    )

    assert queue.acquire_next_job("endpoint_worker", cluster="test-cluster") is None
    assert queue.acquire_job(first.job_id, "endpoint_worker", cluster="test-cluster") is None
    second, lease = queue.submit_and_acquire_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.INPUT_INGEST,
            spec=spec,
            idempotency_key="managed-never-submit-acquire-input",
            metadata=metadata,
        ),
        "endpoint_worker",
    )
    assert lease is None
    assert first.state is JobState.QUEUED
    assert second.state is JobState.QUEUED
    queue.close()


def test_input_ingest_is_not_a_worker_queue_blocker(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    ingest = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.INPUT_INGEST,
            spec=InputArtifactSpec(
                logical_name="input.txt",
                size_bytes=0,
                sha256=hashlib.sha256(b"").hexdigest(),
            ),
            idempotency_key="diagnose-input-ingest",
            metadata=_input_metadata(),
        )
    )
    workload = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["workload"]),
            idempotency_key="diagnose-workload",
        )
    )
    queue.register_endpoint(
        EndpointRegistration(
            endpoint_id="endpoint_worker",
            role=EndpointRole.WORKER,
            cluster="test-cluster",
            hostname="worker",
            pid=1,
            metadata={"concurrency": 1, "kind_concurrency": {}},
        )
    )

    ingest_diagnosis = diagnose_job(queue, ingest.job_id)
    workload_diagnosis = diagnose_job(queue, workload.job_id)
    ingest_admission = cast(dict[str, object], ingest_diagnosis["queue"])["admission"]
    workload_admission = cast(dict[str, object], workload_diagnosis["queue"])["admission"]

    assert ingest_diagnosis["reason"] == "input_ingest_in_progress"
    assert cast(dict[str, object], ingest_admission)["target_ineligibility"] == (
        "internal_input_ingest"
    )
    assert cast(dict[str, object], workload_admission)["target_admissible_now"] is True
    assert cast(dict[str, object], workload_admission)["skipped_predecessors"] == [
        {"job_id": ingest.job_id, "reason": "internal_input_ingest"}
    ]


def test_public_job_route_cannot_submit_an_input_ingest(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    job = RelayJob(
        cluster="test-cluster",
        kind=JobKind.INPUT_INGEST,
        spec=InputArtifactSpec(
            logical_name="input.txt",
            size_bytes=0,
            sha256=hashlib.sha256(b"").hexdigest(),
        ),
        idempotency_key="public-input-ingest",
    )
    client = cast(Any, TestClient(create_app(settings)))

    response = client.post("/jobs", json=job.model_dump(mode="json"))

    assert response.status_code == 422

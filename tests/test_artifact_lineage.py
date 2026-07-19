from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

import clio_relay.core_queue as core_queue_module
from clio_relay.cli import app
from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.http_api import create_app
from clio_relay.mcp_server import handle_request
from clio_relay.models import (
    ArtifactRef,
    ArtifactUse,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    UsedArtifactRef,
)


def _job(
    key: str,
    *,
    used_artifact_refs: list[ArtifactUse] | None = None,
    job_id: str | None = None,
    metadata: Mapping[str, object] | None = None,
) -> RelayJob:
    values: dict[str, object] = {
        "cluster": "test-cluster",
        "kind": JobKind.JARVIS,
        "spec": JarvisRunSpec(command=["true"]),
        "idempotency_key": key,
        "used_artifact_refs": used_artifact_refs or [],
        "metadata": dict(metadata or {}),
    }
    if job_id is not None:
        values["job_id"] = job_id
    return RelayJob.model_validate(values)


def _bind_owned_session_cluster_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Bind the exact test cluster definition to an owned API process."""
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


def _producer_artifact(queue: ClioCoreQueue) -> tuple[RelayJob, ArtifactRef]:
    producer = queue.submit_job(_job("producer"))
    payload = b"immutable scientific input\n"
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=producer.job_id,
            uri="file:///datasets/input.h5",
            kind="dataset",
            size_bytes=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )
    )
    return producer, artifact


def _finish_gc(queue: ClioCoreQueue, job_id: str) -> None:
    for _ in range(100):
        result = queue.collect_terminal_job(
            job_id,
            execute=True,
            batch_size=20,
            external_quarantine_id=f"test:{job_id}",
        )
        if result.complete:
            return
    raise AssertionError("terminal GC did not complete")


def test_submission_validates_digest_and_persists_bidirectional_edge(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None

    tampered = _job(
        "correctable-consumer",
        used_artifact_refs=[ArtifactUse(artifact_id=artifact.artifact_id, sha256="0" * 64)],
    )
    with pytest.raises(QueueConflictError, match="digest mismatch"):
        queue.submit_job(tampered)

    corrected = _job(
        "correctable-consumer",
        used_artifact_refs=[ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)],
    )
    consumer = queue.submit_job(corrected)
    repeated = queue.submit_job(corrected)

    assert repeated.job_id == consumer.job_id
    used, used_cursor, used_total = queue.list_used_artifacts_page(consumer.job_id)
    users, users_cursor, users_total = queue.list_artifact_users_page(artifact.artifact_id)
    assert used == users
    assert used[0].producer_job_id == producer.job_id
    assert used[0].consumer_job_id == consumer.job_id
    assert used[0].sha256 == artifact.sha256
    assert used[0].sequence == 1
    assert used_cursor is None
    assert users_cursor is None
    assert used_total == users_total == 1


def test_core_rejects_cross_session_artifact_use_before_idempotency_reservation(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    for session_id, generation_id in (
        ("desktop-session-a", "generation-a"),
        ("desktop-session-b", "generation-b"),
    ):
        assert (
            queue.prepare_owner_session_start(
                session_id,
                recorded_generation_id=None,
                candidate_generation_id=generation_id,
            )
            == generation_id
        )
    owner_a = {
        "owner": "clio-relay",
        "owner_session_id": "desktop-session-a",
        "owner_session_generation_id": "generation-a",
    }
    owner_b = {
        "owner": "clio-relay",
        "owner_session_id": "desktop-session-b",
        "owner_session_generation_id": "generation-b",
    }
    producer = queue.submit_job(_job("owned-producer", metadata=owner_a))
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=producer.job_id,
            uri="file:///owned/input.h5",
            kind="dataset",
            sha256=hashlib.sha256(b"owned input").hexdigest(),
        )
    )
    assert artifact.sha256 is not None
    foreign = _job(
        "foreign-consumer",
        metadata=owner_b,
        used_artifact_refs=[ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)],
    )

    with pytest.raises(QueueConflictError, match="owner session generation"):
        queue.submit_job(foreign)

    assert queue.resolve_idempotent_submission(foreign).state == "new"
    same_owner = queue.submit_job(
        _job(
            "same-owner-consumer",
            metadata=owner_a,
            used_artifact_refs=[
                ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)
            ],
        )
    )
    assert same_owner.used_artifact_refs


def test_owned_http_submission_rejects_other_session_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    for session_id, generation_id in (
        ("desktop-session-a", "generation-a"),
        ("desktop-session-b", "generation-b"),
    ):
        assert (
            queue.prepare_owner_session_start(
                session_id,
                recorded_generation_id=None,
                candidate_generation_id=generation_id,
            )
            == generation_id
        )
    producer = queue.submit_job(
        _job(
            "http-other-producer",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-b",
                "owner_session_generation_id": "generation-b",
            },
        )
    )
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=producer.job_id,
            uri="file:///owned/http-input.h5",
            kind="dataset",
            sha256=hashlib.sha256(b"other session input").hexdigest(),
        )
    )
    assert artifact.sha256 is not None
    settings = RelaySettings(
        core_dir=core_dir,
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-a",
        owner_session_generation_id="generation-a",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                "X-Clio-Relay-Owner-Session-Id": "desktop-session-a",
                "X-Clio-Relay-Session-Generation-Id": "generation-a",
            },
        ),
    )

    response = client.post(
        "/jobs/jarvis",
        json={
            "cluster": "test-cluster",
            "pipeline_yaml": "name: owner-lock\npkgs: []\n",
            "idempotency_key": "http-foreign-consumer",
            "used_artifact_refs": [
                {"artifact_id": artifact.artifact_id, "sha256": artifact.sha256}
            ],
        },
    )

    assert response.status_code == 403
    assert all(job.idempotency_key != "http-foreign-consumer" for job in queue.list_jobs())


def test_empty_lineage_preserves_pre_upgrade_idempotency_digest() -> None:
    job = _job("legacy-no-lineage")
    legacy_payload = job.model_dump(mode="json")
    for generated_field in {
        "job_id",
        "state",
        "created_at",
        "updated_at",
        "leased_by",
        "attempts",
        "last_error",
        "submission_digest",
        "used_artifact_refs",
    }:
        legacy_payload.pop(generated_field, None)
    expected = hashlib.sha256(
        json.dumps(legacy_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    assert (
        core_queue_module._job_idempotency_digest(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job
        )
        == expected
    )


def test_artifact_user_queries_are_bounded_paginated_and_identifier_safe(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    _producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    consumers = [
        queue.submit_job(
            _job(
                f"consumer-{index}",
                used_artifact_refs=[
                    ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)
                ],
            )
        )
        for index in range(3)
    ]

    first, cursor, total = queue.list_artifact_users_page(artifact.artifact_id, limit=2)
    assert len(first) == 2
    assert cursor == "edge_00000000000000000002"
    assert total == 3
    assert cursor is not None
    second, next_cursor, second_total = queue.list_artifact_users_page(
        artifact.artifact_id,
        cursor=cursor,
        limit=2,
    )
    assert len(second) == 1
    assert next_cursor is None
    assert second_total == 3
    assert {item.consumer_job_id for item in [*first, *second]} == {
        consumer.job_id for consumer in consumers
    }

    with pytest.raises(ValueError, match="invalid artifact_id"):
        queue.list_artifact_users_page("../artifact_escape")
    with pytest.raises(ValueError, match="invalid cursor"):
        queue.list_artifact_users_page(artifact.artifact_id, cursor="../job_escape")


def test_artifact_user_cursor_is_monotonic_across_interleaved_insert_and_gc(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    _producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    pin = ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)
    first_consumer = queue.submit_job(_job("cursor-first", used_artifact_refs=[pin]))
    second_consumer = queue.submit_job(_job("cursor-second", used_artifact_refs=[pin]))

    first_page, cursor, total = queue.list_artifact_users_page(artifact.artifact_id, limit=1)
    assert [record.consumer_job_id for record in first_page] == [first_consumer.job_id]
    assert cursor == "edge_00000000000000000001"
    assert total == 2

    late_consumer = queue.submit_job(
        _job(
            "cursor-lexically-earlier",
            job_id="job_00000000000000000000000000000000",
            used_artifact_refs=[pin],
        )
    )
    queue.update_job_state(first_consumer.job_id, JobState.SUCCEEDED)
    _finish_gc(queue, first_consumer.job_id)

    assert cursor is not None
    remaining, next_cursor, remaining_total = queue.list_artifact_users_page(
        artifact.artifact_id,
        cursor=cursor,
        limit=10,
    )
    assert [record.consumer_job_id for record in remaining] == [
        second_consumer.job_id,
        late_consumer.job_id,
    ]
    assert [record.sequence for record in remaining] == [2, 3]
    assert next_cursor is None
    assert remaining_total == 2


def test_submission_refuses_to_overfill_bounded_reverse_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(core_queue_module, "MAX_ARTIFACT_CONSUMERS", 2)
    queue = ClioCoreQueue(tmp_path)
    _producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    pin = ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)

    for index in range(2):
        queue.submit_job(_job(f"bounded-consumer-{index}", used_artifact_refs=[pin]))
    overflow = _job("bounded-consumer-overflow", used_artifact_refs=[pin])
    with pytest.raises(QueueConflictError, match="consumer capacity is exhausted"):
        queue.submit_job(overflow)
    assert not (
        queue.root / "used_artifacts_by_job" / overflow.job_id / f"{artifact.artifact_id}.json"
    ).exists()

    records, next_cursor, total = queue.list_artifact_users_page(
        artifact.artifact_id,
        limit=2,
    )
    assert len(records) == total == 2
    assert next_cursor is None


def test_reserved_submission_recovers_partial_lineage_edge_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    _producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    pin = ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)
    first = _job("recover-partial-lineage", used_artifact_refs=[pin])
    real_write_job = queue._write_job_unlocked  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    failed_once = False

    def fail_after_forward_edge(job: RelayJob) -> None:
        nonlocal failed_once
        if not failed_once and job.job_id == first.job_id:
            failed_once = True
            raise OSError("simulated failure after lineage edge persistence")
        real_write_job(job)

    monkeypatch.setattr(queue, "_write_job_unlocked", fail_after_forward_edge)
    with pytest.raises(OSError, match="simulated failure"):
        queue.submit_job(first)

    forward_path = (
        queue.root / "used_artifacts_by_job" / first.job_id / f"{artifact.artifact_id}.json"
    )
    assert forward_path.is_file()
    first_edge_created_at = json.loads(forward_path.read_text(encoding="utf-8"))["created_at"]

    recovered = queue.submit_job(_job("recover-partial-lineage", used_artifact_refs=[pin]))
    assert recovered.job_id == first.job_id
    forward, _cursor, total = queue.list_used_artifacts_page(recovered.job_id)
    reverse, _reverse_cursor, reverse_total = queue.list_artifact_users_page(artifact.artifact_id)
    assert total == reverse_total == 1
    assert forward == reverse
    assert forward[0].sequence == 1
    assert forward[0].model_dump(mode="json")["created_at"] == first_edge_created_at


def test_reserved_submission_recovers_after_monotonic_counter_crash_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    _producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    pin = ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)
    first = _job("recover-counter-gap", used_artifact_refs=[pin])
    real_write = queue._write_immutable_artifact_use_record  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    failed_once = False

    def fail_before_mapping(path: Path, record: UsedArtifactRef) -> None:
        nonlocal failed_once
        if not failed_once and path.parent.name == "by_consumer":
            failed_once = True
            raise OSError("simulated failure after counter advance")
        real_write(path, record)

    monkeypatch.setattr(queue, "_write_immutable_artifact_use_record", fail_before_mapping)
    with pytest.raises(OSError, match="counter advance"):
        queue.submit_job(first)

    recovered = queue.submit_job(_job("recover-counter-gap", used_artifact_refs=[pin]))
    forward, _cursor, total = queue.list_used_artifacts_page(recovered.job_id)
    assert total == 1
    assert forward[0].sequence == 2
    head = json.loads(
        (queue.root / "artifact_user_order" / artifact.artifact_id / "head.json").read_text(
            encoding="utf-8"
        )
    )
    assert head["latest_sequence"] == 2


def test_job_index_migration_resolves_canonical_artifact_before_artifact_indexes(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    consumer = queue.submit_job(
        _job(
            "migration-consumer",
            used_artifact_refs=[
                ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)
            ],
        )
    )
    forward_path = (
        queue.root / "used_artifacts_by_job" / consumer.job_id / f"{artifact.artifact_id}.json"
    )
    reverse_path = queue.root / "artifact_users" / artifact.artifact_id / f"{consumer.job_id}.json"
    forward_path.unlink()
    reverse_path.unlink()
    (queue.root / "artifacts_by_job" / producer.job_id / f"{artifact.artifact_id}.json").unlink()

    with queue._lock:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._migrate_record_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "jobs",
            consumer,
        )

    assert forward_path.is_file()
    assert reverse_path.is_file()
    assert queue.list_used_artifacts_page(consumer.job_id)[2] == 1


def test_gc_protects_producer_until_retained_consumer_is_collected(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    consumer = queue.submit_job(
        _job(
            "retained-consumer",
            used_artifact_refs=[
                ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)
            ],
        )
    )
    queue.update_job_state(producer.job_id, JobState.SUCCEEDED)
    queue.update_job_state(consumer.job_id, JobState.SUCCEEDED)

    protected = queue.plan_terminal_job_gc(producer.job_id)
    assert protected.eligible is False
    assert "artifact_used_by_retained_job" in protected.protections

    _finish_gc(queue, consumer.job_id)
    eligible = queue.plan_terminal_job_gc(producer.job_id)
    assert eligible.eligible is True
    assert "artifact_used_by_retained_job" not in eligible.protections


def test_http_submission_and_lineage_queries_share_the_content_pin(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    _producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    client = cast(Any, TestClient(create_app(settings)))

    submitted = client.post(
        "/jobs/jarvis",
        json={
            "cluster": "test-cluster",
            "pipeline_yaml": "name: lineage-http\npkgs: []\n",
            "idempotency_key": "lineage-http",
            "used_artifact_refs": [
                {"artifact_id": artifact.artifact_id, "sha256": artifact.sha256}
            ],
        },
    )
    assert submitted.status_code == 200
    consumer_id = submitted.json()["job_id"]

    used = client.get(f"/jobs/{consumer_id}/used-artifacts", params={"limit": 1})
    used_by = client.get(f"/artifacts/{artifact.artifact_id}/used-by", params={"limit": 1})

    assert used.status_code == 200
    assert used_by.status_code == 200
    assert used.json()["used_artifacts"][0]["consumer_job_id"] == consumer_id
    assert used_by.json()["used_by"] == used.json()["used_artifacts"]


def test_cli_submits_and_queries_content_pinned_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    _producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: lineage-cli\npkgs: []\n", encoding="utf-8")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    def require_local_cluster(_cluster: str) -> object:
        return object()

    def execute_locally(_definition: object) -> bool:
        return False

    monkeypatch.setattr("clio_relay.cli._require_cluster", require_local_cluster)
    monkeypatch.setattr(
        "clio_relay.cli.should_execute_on_cluster",
        execute_locally,
    )

    submitted = CliRunner().invoke(
        app,
        [
            "job",
            "submit",
            "--cluster",
            "test-cluster",
            "--jarvis-yaml",
            str(pipeline),
            "--used-artifact",
            f"{artifact.artifact_id}={artifact.sha256}",
        ],
    )
    assert submitted.exit_code == 0
    consumer_id = submitted.output.strip()
    assert queue.get_job(consumer_id).used_artifact_refs == [
        ArtifactUse(artifact_id=artifact.artifact_id, sha256=artifact.sha256)
    ]

    forward = CliRunner().invoke(app, ["job", "used-artifacts", consumer_id])
    reverse = CliRunner().invoke(app, ["job", "used-by", artifact.artifact_id])
    assert forward.exit_code == reverse.exit_code == 0
    forward_payload = cast(dict[str, Any], json.loads(forward.output))
    reverse_payload = cast(dict[str, Any], json.loads(reverse.output))
    assert forward_payload["used_artifacts"] == reverse_payload["used_by"]
    assert forward_payload["used_artifacts"][0]["consumer_job_id"] == consumer_id


def test_user_mcp_can_submit_and_query_artifact_lineage(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    _producer, artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    pin = {"artifact_id": artifact.artifact_id, "sha256": artifact.sha256}
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 0, "method": "tools/list"},
        queue=queue,
        settings=settings,
        profile="user",
    )
    assert listed is not None
    tool_names = {tool["name"] for tool in listed["result"]["tools"]}
    assert "relay_artifact_lineage" in tool_names
    assert "relay_used_artifacts" not in tool_names
    assert "relay_used_by" not in tool_names

    submitted = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_agent",
                "arguments": {
                    "cluster": "test-cluster",
                    "prompt_path": "/work/prompt.md",
                    "idempotency_key": "lineage-mcp",
                    "used_artifact_refs": [pin],
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )
    assert submitted is not None
    consumer_id = submitted["result"]["structuredContent"]["job_id"]
    queried = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "relay_artifact_lineage",
                "arguments": {"artifact_id": artifact.artifact_id, "limit": 1},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )

    assert queried is not None
    edge = queried["result"]["structuredContent"]["used_by"][0]
    assert edge["consumer_job_id"] == consumer_id
    assert edge["artifact_id"] == artifact.artifact_id
    assert edge["sha256"] == artifact.sha256
    forward = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "relay_artifact_lineage",
                "arguments": {"job_id": consumer_id, "limit": 1},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )
    assert forward is not None
    assert forward["result"]["structuredContent"]["used_artifacts"] == [edge]

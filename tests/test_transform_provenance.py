from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

import clio_relay.cli as cli_module
import clio_relay.core_queue as core_queue_module
import clio_relay.mcp_server as mcp_server_module
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.http_api import create_app
from clio_relay.input_staging import merge_artifact_uses
from clio_relay.mcp_server import handle_request
from clio_relay.models import (
    ArtifactMechanism,
    ArtifactRef,
    ArtifactUse,
    ArtifactUseEvidence,
    ArtifactUseProvenance,
    JarvisPipelineInputLineage,
    JarvisPipelineInputRoute,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    TransformEnvironment,
    TransformEnvironmentTier,
    TransformRef,
    TransformReplayContract,
    TransformUseEvidence,
    UsedArtifactRef,
    artifact_use_payload,
    validate_artifact_use_collection,
)


def _job(
    key: str,
    *,
    used_artifact_refs: list[ArtifactUse] | None = None,
) -> RelayJob:
    return RelayJob(
        cluster="test-cluster",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=key,
        used_artifact_refs=used_artifact_refs or [],
    )


def _producer_artifact(queue: ClioCoreQueue) -> ArtifactRef:
    producer = queue.submit_job(_job("transform-producer"))
    content = b"content-pinned transform input\n"
    return queue.append_artifact(
        ArtifactRef(
            job_id=producer.job_id,
            uri="file:///shared/input.h5",
            kind="dataset",
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
    )


def _tool_result(response: dict[str, Any] | None) -> dict[str, Any]:
    assert response is not None
    return cast(dict[str, Any], response["result"]["structuredContent"])


def test_legacy_artifact_use_wire_and_digests_remain_stable() -> None:
    pin = ArtifactUse(artifact_id="artifact_input", sha256="a" * 64)
    old_wire = {"artifact_id": "artifact_input", "sha256": "a" * 64}

    assert artifact_use_payload(pin) == old_wire
    assert (
        UsedArtifactRef.model_validate(
            {
                "artifact_id": "artifact_input",
                "consumer_job_id": "job_consumer",
                "producer_job_id": "job_producer",
                "sequence": 1,
                "sha256": "a" * 64,
                "created_at": "2026-07-22T00:00:00Z",
            }
        ).provenance
        is None
    )
    assert cli_module._artifact_use_refs(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        [f"{pin.artifact_id}={pin.sha256}"]
    ) == [pin]
    assert (
        cli_module._artifact_use_cli_value(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            pin
        )
        == f"{pin.artifact_id}={pin.sha256}"
    )

    job = _job("legacy-nonempty-lineage", used_artifact_refs=[pin])
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
    }:
        legacy_payload.pop(generated_field, None)
    legacy_payload["used_artifact_refs"] = [old_wire]
    expected_job_digest = hashlib.sha256(
        json.dumps(legacy_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert (
        core_queue_module._job_idempotency_digest(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job
        )
        == expected_job_digest
    )

    route = JarvisPipelineInputRoute(
        cluster="test-cluster",
        server_name="jarvis",
        cluster_route_revision="1" * 64,
        registration_revision="2" * 64,
        expected_server_artifact_digest="3" * 64,
        pipeline_id="legacy-lineage",
        owner_session_id="desktop-session",
        owner_session_generation_id="generation_legacy",
    )
    lineage = JarvisPipelineInputLineage.create(
        route=route,
        artifact_uses=(pin,),
        manifest_sha256s=("4" * 64,),
    )
    legacy_lineage = lineage.model_dump(mode="json", exclude={"document_sha256"})
    legacy_lineage["artifact_uses"] = [old_wire]
    expected_lineage_digest = hashlib.sha256(
        json.dumps(legacy_lineage, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert lineage.document_sha256 == expected_lineage_digest
    assert (
        JarvisPipelineInputLineage.model_validate(
            {**legacy_lineage, "document_sha256": expected_lineage_digest}
        )
        == lineage
    )


def test_provenance_json_is_canonical_and_aggregate_bounded() -> None:
    provenance = ArtifactUseProvenance(
        evidence=ArtifactUseEvidence.HASH_PAIR,
        arg="input_file",
        note="reconciled",
    )
    pin = ArtifactUse(
        artifact_id="artifact_input",
        sha256="a" * 64,
        provenance=provenance,
    )
    expected = json.dumps(
        artifact_use_payload(pin),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )

    assert (
        cli_module._artifact_use_cli_value(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            pin
        )
        == expected
    )
    assert (
        mcp_server_module._artifact_use_cli_value(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            pin
        )
        == expected
    )
    assert cli_module._artifact_use_refs(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        [expected]
    ) == [pin]

    with pytest.raises(ValidationError, match="authority evidence requires"):
        ArtifactUseProvenance(evidence=ArtifactUseEvidence.AUTHORITY)

    large_provenance = ArtifactUseProvenance(
        evidence=ArtifactUseEvidence.ASSERTION,
        authority="a" * 3_000,
        external_ref="e" * 3_000,
        arg="g" * 512,
        note="n" * 512,
    )
    large_collection = [
        ArtifactUse(
            artifact_id=f"artifact_large_{index}",
            sha256=f"{index:064x}",
            provenance=large_provenance,
        )
        for index in range(40)
    ]
    with pytest.raises(ValueError, match="artifact-use collection exceeds"):
        validate_artifact_use_collection(large_collection)


def test_full_provenance_record_controls_merge_and_durable_edges(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    artifact = _producer_artifact(queue)
    assert artifact.sha256 is not None
    provenance = ArtifactUseProvenance(
        evidence=ArtifactUseEvidence.SCHEMA_ARG,
        arg="input_file",
    )
    pin = ArtifactUse(
        artifact_id=artifact.artifact_id,
        sha256=artifact.sha256,
        provenance=provenance,
    )
    consumer = queue.submit_job(_job("transform-consumer", used_artifact_refs=[pin]))

    used, _, _ = queue.list_used_artifacts_page(consumer.job_id)
    users, _, _ = queue.list_artifact_users_page(artifact.artifact_id)
    assert used == users
    assert used[0].provenance == provenance

    changed_pin = pin.model_copy(
        update={
            "provenance": ArtifactUseProvenance(
                evidence=ArtifactUseEvidence.ASSERTION,
                note="untrusted",
            )
        }
    )
    with pytest.raises(ValueError, match="identity changed"):
        merge_artifact_uses([pin], (changed_pin,))
    with pytest.raises(QueueConflictError, match="idempotency key was reused"):
        queue.submit_job(_job("transform-consumer", used_artifact_refs=[changed_pin]))

    assert queue.get_transform_ref(consumer.job_id) is None
    transform = TransformRef(
        job_id=consumer.job_id,
        activity_id="call_add_step",
        mechanism=ArtifactMechanism.TOOL_SCHEMA,
        environment=TransformEnvironment(
            tier=TransformEnvironmentTier.LOCKFILE_HASH,
            clio_version="0.7.11",
            lockfile_sha256="b" * 64,
            launcher_fingerprint="123:456",
            provider_id="anthropic",
            model_id="claude-haiku",
            os="Windows",
            arch="AMD64",
            python_version="3.11.9",
        ),
        replay=TransformReplayContract.REPRODUCIBLE,
        used_evidence=(
            TransformUseEvidence(
                evidence=ArtifactUseEvidence.SCHEMA_ARG,
                artifact_id=artifact.artifact_id,
                sha256=artifact.sha256,
                arg="input_file",
            ),
        ),
    )
    assert queue.record_transform_ref(transform) == transform
    assert queue.record_transform_ref(transform) == transform
    assert queue.get_transform_ref(consumer.job_id) == transform

    changed_transform = transform.model_copy(update={"replay_reason": "changed"})
    with pytest.raises(QueueConflictError, match="immutable transform ref changed"):
        queue.record_transform_ref(changed_transform)
    mismatched_edge = TransformUseEvidence(
        evidence=ArtifactUseEvidence.ASSERTION,
        artifact_id=artifact.artifact_id,
        sha256=artifact.sha256,
        note="untrusted",
    )
    with pytest.raises(QueueConflictError, match="provenance changed"):
        queue.record_transform_ref(
            transform.model_copy(update={"used_evidence": (mismatched_edge,)})
        )

    recovered = ClioCoreQueue(tmp_path / "core")
    assert recovered.get_transform_ref(consumer.job_id) == transform


def test_transform_supports_authority_only_and_zero_input_jobs(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="requires lockfile_sha256"):
        TransformEnvironment(tier=TransformEnvironmentTier.LOCKFILE_HASH)
    with pytest.raises(ValidationError, match="requires image_digest"):
        TransformEnvironment(tier=TransformEnvironmentTier.IMAGE_DIGEST)

    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(_job("authority-only-transform"))
    transform = TransformRef(
        job_id=job.job_id,
        activity_id="catalog_query",
        mechanism=ArtifactMechanism.TOOL_SCHEMA,
        replay=TransformReplayContract.RE_RUNNABLE,
        replay_reason="authority input has no local content hash",
        used_evidence=(
            TransformUseEvidence(
                evidence=ArtifactUseEvidence.AUTHORITY,
                authority="doi:10.1234/example",
                external_ref="https://catalog.example/dataset/42",
                arg="dataset_id",
            ),
        ),
    )

    assert queue.record_transform_ref(transform) == transform
    assert queue.get_transform_ref(job.job_id) == transform


def test_transform_family_is_an_additive_sealed_queue_upgrade(tmp_path: Path) -> None:
    core_dir = tmp_path / "core"
    ClioCoreQueue(core_dir).initialize()
    transform_dir = core_dir / "transforms"
    assert transform_dir.is_dir()

    # This is the exact fixed layout produced before the transform family existed.
    transform_dir.rmdir()
    ClioCoreQueue(core_dir).initialize()

    assert transform_dir.is_dir()


def test_terminal_job_gc_collects_transform_with_its_job(tmp_path: Path) -> None:
    """A transform cannot outlive the relay job whose activity it describes."""
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(_job("transform-gc"))
    transform = TransformRef(
        job_id=job.job_id,
        activity_id="zero-input-transform",
        mechanism=ArtifactMechanism.HARNESS,
    )
    queue.record_transform_ref(transform)
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)

    for _ in range(100):
        result = queue.collect_terminal_job(
            job.job_id,
            execute=True,
            batch_size=3,
            external_quarantine_id=f"test-quarantine:{job.job_id}",
        )
        if result.complete:
            break
    else:
        raise AssertionError("terminal transform GC did not complete")

    assert not (tmp_path / "core" / "transforms" / f"{job.job_id}.json").exists()
    with pytest.raises(NotFoundError):
        queue.get_transform_ref(job.job_id)


def test_authenticated_http_and_existing_mcp_get_status_expose_transform(
    tmp_path: Path,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(_job("http-transform"))
    settings = RelaySettings(
        core_dir=core_dir,
        spool_dir=tmp_path / "spool",
        api_token="secret-token",
    )
    headers = {"X-Clio-Relay-Token": "secret-token"}
    transform = TransformRef(
        job_id=job.job_id,
        activity_id="relay-job",
        mechanism=ArtifactMechanism.HARNESS,
    )

    with cast(Any, TestClient(create_app(settings))) as client:
        assert client.get(f"/jobs/{job.job_id}/transform").status_code == 401
        empty = client.get(f"/jobs/{job.job_id}/transform", headers=headers)
        empty_status = client.get(f"/jobs/{job.job_id}/status", headers=headers)
        assert empty.status_code == 200
        assert empty.json() is None
        assert empty_status.json()["transform"] is None

        recorded = client.post(
            f"/jobs/{job.job_id}/transform",
            headers=headers,
            json=transform.model_dump(mode="json"),
        )
        fetched = client.get(f"/jobs/{job.job_id}/transform", headers=headers)
        status = client.get(f"/jobs/{job.job_id}/status", headers=headers)
        assert recorded.status_code == fetched.status_code == status.status_code == 200
        assert recorded.json() == fetched.json() == transform.model_dump(mode="json")
        assert status.json()["transform"] == transform.model_dump(mode="json")

        conflict = client.post(
            f"/jobs/{job.job_id}/transform",
            headers=headers,
            json=transform.model_copy(update={"replay_reason": "changed"}).model_dump(mode="json"),
        )
        missing = client.get("/jobs/job_missing/transform", headers=headers)
        assert conflict.status_code == 409
        assert missing.status_code == 404

    mcp_queue = ClioCoreQueue(core_dir)
    get_result = _tool_result(
        handle_request(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "relay_get_job",
                    "arguments": {"job_id": job.job_id},
                },
            },
            queue=mcp_queue,
            settings=settings,
            profile="admin",
        )
    )
    status_result = _tool_result(
        handle_request(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "relay_get_job_status",
                    "arguments": {"job_id": job.job_id},
                },
            },
            queue=mcp_queue,
            settings=settings,
            profile="admin",
        )
    )
    expected = transform.model_dump(mode="json")
    assert get_result["transform"] == expected
    assert status_result["transform"] == expected

    listed = handle_request(
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
        queue=mcp_queue,
        settings=settings,
        profile="admin",
    )
    assert listed is not None
    names = {item["name"] for item in listed["result"]["tools"]}
    assert "relay_record_transform" not in names

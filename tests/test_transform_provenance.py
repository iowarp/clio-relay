from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError
from clio_relay.models import (
    ArtifactMechanism,
    ArtifactUse,
    ArtifactUseEvidence,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    TransformEnvironment,
    TransformEnvironmentTier,
    TransformRef,
    TransformReplayContract,
    TransformUseEvidence,
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

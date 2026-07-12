from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import JarvisRunSpec, JobKind, JobState, RelayJob


def _job(key: str) -> RelayJob:
    return RelayJob(
        cluster="test-cluster",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=key,
    )


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


def test_idempotency_resolution_recovers_canonical_crash_reserved_job_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    original = _job("crash-reserved")
    fresh = queue.resolve_idempotent_submission(original)
    assert fresh.state == "new"
    assert fresh.canonical_job_id == original.job_id
    assert fresh.existing_job is None

    original_ensure = queue._ensure_global_order_entry_unlocked  # pyright: ignore[reportPrivateUsage]

    def fail_before_job_write(_family: str, _record_id: str) -> int:
        raise RuntimeError("fault after idempotency reservation")

    monkeypatch.setattr(queue, "_ensure_global_order_entry_unlocked", fail_before_job_write)
    with pytest.raises(RuntimeError, match="fault after idempotency reservation"):
        queue.submit_job(original)
    monkeypatch.setattr(queue, "_ensure_global_order_entry_unlocked", original_ensure)

    retry_candidate = _job("crash-reserved")
    assert retry_candidate.job_id != original.job_id
    reserved = queue.resolve_idempotent_submission(retry_candidate)
    assert reserved.state == "reserved"
    assert reserved.canonical_job_id == original.job_id
    assert reserved.existing_job is None

    canonical_retry = retry_candidate.model_copy(update={"job_id": reserved.canonical_job_id})
    submitted = queue.submit_job(canonical_retry)
    assert submitted.job_id == original.job_id
    existing = queue.resolve_idempotent_submission(retry_candidate)
    assert existing.state == "existing"
    assert existing.canonical_job_id == original.job_id
    assert existing.existing_job == submitted


def test_idempotency_resolution_returns_retired_replay_and_rejects_missing_commit(
    tmp_path: Path,
) -> None:
    retired_queue = ClioCoreQueue(tmp_path / "retired")
    intent = _job("retired-resolution")
    submitted = retired_queue.submit_job(intent)
    retired_queue.update_job_state(submitted.job_id, JobState.SUCCEEDED)
    _finish_gc(retired_queue, submitted.job_id)

    retired = retired_queue.resolve_idempotent_submission(intent)
    assert retired.state == "retired"
    assert retired.canonical_job_id == submitted.job_id
    assert retired.existing_job is not None
    assert retired.existing_job.state is JobState.SUCCEEDED

    missing_queue = ClioCoreQueue(tmp_path / "missing")
    missing_intent = _job("missing-committed-target")
    missing = missing_queue.submit_job(missing_intent)
    (missing_queue.root / "jobs" / f"{missing.job_id}.json").unlink()
    with pytest.raises(QueueConflictError, match="points to missing job"):
        missing_queue.resolve_idempotent_submission(missing_intent)


def test_idempotency_resolution_rejects_payload_mismatch(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    queue.submit_job(_job("resolution-mismatch"))
    changed = _job("resolution-mismatch").model_copy(update={"cluster": "other-cluster"})
    with pytest.raises(QueueConflictError, match="different or invalid job payload"):
        queue.resolve_idempotent_submission(changed)

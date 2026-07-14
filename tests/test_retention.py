from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

import clio_relay.retention as retention_module
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import JarvisRunSpec, JobKind, JobState, RelayJob
from clio_relay.retention import (
    SpoolRetentionPhase,
    TerminalRetentionCoordinator,
)
from clio_relay.spool import JobSpool


def _terminal_job_with_spool(tmp_path: Path, key: str) -> tuple[ClioCoreQueue, RelayJob, Path]:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key=key,
        )
    )
    spool_root = tmp_path / "spool"
    spool = JobSpool(spool_root, job)
    spool.initialize()
    (spool.path / "result.txt").write_text("retained until core retirement", encoding="utf-8")
    terminal = queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    return queue, terminal, spool_root


def _finish_retention(
    coordinator: TerminalRetentionCoordinator,
    job_id: str,
    *,
    batch_size: int = 7,
) -> None:
    for _ in range(1_000):
        result = coordinator.collect(job_id, execute=True, batch_size=batch_size)
        assert result.actions <= batch_size
        assert result.scheduler_cancel_requested is False
        if result.complete:
            return
    raise AssertionError("retention did not complete within bounded batches")


def test_outer_retention_is_dry_run_then_quarantines_before_core_collection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, job, spool_root = _terminal_job_with_spool(tmp_path, "outer-retention-order")
    coordinator = TerminalRetentionCoordinator(queue, spool_root)

    preview = coordinator.collect(job.job_id)
    assert preview.dry_run is True
    assert preview.plan.eligible is True
    assert (spool_root / job.job_id).is_dir()
    assert not (spool_root / ".retention").exists()
    assert queue.get_job(job.job_id).state is JobState.SUCCEEDED

    original_collect = queue.collect_terminal_job
    observed_quarantine_ids: list[str] = []

    def assert_quarantined_first(*args: Any, **kwargs: Any) -> Any:
        quarantine_id = kwargs.get("external_quarantine_id")
        assert isinstance(quarantine_id, str)
        observed_quarantine_ids.append(quarantine_id)
        assert not (spool_root / job.job_id).exists()
        assert (spool_root / ".retention" / "quarantine" / quarantine_id).is_dir()
        return original_collect(*args, **kwargs)

    monkeypatch.setattr(queue, "collect_terminal_job", assert_quarantined_first)
    _finish_retention(coordinator, job.job_id)

    assert observed_quarantine_ids
    tombstone = queue.get_job_tombstone(job.job_id)
    assert tombstone is not None
    assert tombstone.external_quarantine_id == observed_quarantine_ids[0]
    assert not (spool_root / job.job_id).exists()
    assert not (spool_root / ".retention" / "quarantine" / observed_quarantine_ids[0]).exists()
    final = coordinator.collect(job.job_id, execute=True)
    assert final.complete is True
    assert final.receipt is not None
    assert final.receipt.phase is SpoolRetentionPhase.COMPLETE
    final.model_dump_json()


@pytest.mark.parametrize("fault_phase", list(SpoolRetentionPhase))
def test_outer_retention_resumes_after_every_durable_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_phase: SpoolRetentionPhase,
) -> None:
    queue, job, spool_root = _terminal_job_with_spool(
        tmp_path / fault_phase.value,
        f"outer-fault-{fault_phase.value}",
    )
    coordinator = TerminalRetentionCoordinator(queue, spool_root)
    injected = False

    def fail_after_checkpoint(phase: SpoolRetentionPhase) -> None:
        nonlocal injected
        if phase is fault_phase and not injected:
            injected = True
            raise RuntimeError(f"fault after {phase.value}")

    monkeypatch.setattr(coordinator, "_after_retention_checkpoint", fail_after_checkpoint)
    with pytest.raises(RuntimeError, match=f"fault after {fault_phase.value}"):
        for _ in range(1_000):
            coordinator.collect(job.job_id, execute=True, batch_size=1)
    assert injected is True

    resumed = TerminalRetentionCoordinator(queue, spool_root)
    _finish_retention(resumed, job.job_id, batch_size=3)
    tombstone = queue.get_job_tombstone(job.job_id)
    assert tombstone is not None
    result = resumed.collect(job.job_id, execute=True)
    assert result.complete is True
    assert result.receipt is not None
    assert tombstone.external_quarantine_id == result.receipt.receipt_id


def test_outer_retention_recovers_rename_before_receipt_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, job, spool_root = _terminal_job_with_spool(tmp_path, "outer-rename-crash")
    coordinator = TerminalRetentionCoordinator(queue, spool_root)
    injected = False

    def fail_after_rename(_receipt: object) -> None:
        nonlocal injected
        if not injected:
            injected = True
            raise RuntimeError("fault after spool rename")

    monkeypatch.setattr(coordinator, "_after_spool_rename", fail_after_rename)
    with pytest.raises(RuntimeError, match="fault after spool rename"):
        coordinator.collect(job.job_id, execute=True)
    assert not (spool_root / job.job_id).exists()
    plan = coordinator.plan(job.job_id)
    assert plan.receipt_phase is SpoolRetentionPhase.PREPARED
    assert plan.receipt_id is not None
    assert (spool_root / ".retention" / "quarantine" / plan.receipt_id).is_dir()

    _finish_retention(TerminalRetentionCoordinator(queue, spool_root), job.job_id)


def test_outer_retention_fails_closed_on_source_swap_before_anchored_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, job, spool_root = _terminal_job_with_spool(tmp_path, "outer-source-swap")
    coordinator = TerminalRetentionCoordinator(queue, spool_root)
    original_rename = retention_module._rename_owned_child  # pyright: ignore[reportPrivateUsage]
    captured = spool_root / f"{job.job_id}.captured"
    replacement_marker = spool_root / job.job_id / "replacement.txt"

    def swap_before_rename(*args: Any, **kwargs: Any) -> None:
        (spool_root / job.job_id).replace(captured)
        (spool_root / job.job_id).mkdir()
        replacement_marker.write_text("must not be moved or purged", encoding="utf-8")
        original_rename(*args, **kwargs)

    monkeypatch.setattr(retention_module, "_rename_owned_child", swap_before_rename)
    with pytest.raises(QueueConflictError, match="identity changed"):
        coordinator.collect(job.job_id, execute=True)
    assert captured.is_dir()
    assert replacement_marker.read_text(encoding="utf-8") == "must not be moved or purged"
    assert queue.get_job(job.job_id).state is JobState.SUCCEEDED
    assert queue.get_job_tombstone(job.job_id) is None


def test_outer_retention_rejects_symlink_or_junction_spool_source(tmp_path: Path) -> None:
    queue, job, spool_root = _terminal_job_with_spool(tmp_path, "outer-reparse-source")
    original = spool_root / f"{job.job_id}.original"
    (spool_root / job.job_id).replace(original)
    outside = tmp_path / "outside"
    outside.mkdir()
    marker = outside / "marker.txt"
    marker.write_text("outside must remain", encoding="utf-8")
    redirected = spool_root / job.job_id
    if os.name == "nt":
        subprocess.run(
            ["cmd.exe", "/d", "/c", "mklink", "/J", str(redirected), str(outside)],
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        redirected.symlink_to(outside, target_is_directory=True)
    try:
        result = TerminalRetentionCoordinator(queue, spool_root).collect(
            job.job_id,
            execute=True,
        )
        assert result.plan.eligible is False
        assert "spool_source_unsafe" in result.plan.protections
        assert marker.read_text(encoding="utf-8") == "outside must remain"
        assert queue.get_job_tombstone(job.job_id) is None
    finally:
        if os.name == "nt":
            redirected.rmdir()
        else:
            redirected.unlink()


def test_outer_retention_purge_is_bounded_past_501_spool_entries(tmp_path: Path) -> None:
    queue, job, spool_root = _terminal_job_with_spool(tmp_path, "outer-large-purge")
    for index in range(502):
        (spool_root / job.job_id / f"record-{index:04d}.json").write_text(
            "{}",
            encoding="utf-8",
        )
    coordinator = TerminalRetentionCoordinator(queue, spool_root)
    calls = 0
    while True:
        result = coordinator.collect(job.job_id, execute=True, batch_size=100)
        calls += 1
        assert result.actions <= 100
        assert result.scheduler_cancel_requested is False
        if result.complete:
            break
        assert calls < 100
    assert calls > 5
    assert result.receipt is not None
    assert result.receipt.purged_entries > 501

from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    StorageReservationEstimate,
)
from clio_relay.storage_policy import StorageLimits
from clio_relay.storage_runtime import StorageRuntime, StorageRuntimeConfig


class _GrowingProvider(JarvisCdProvider):
    def __init__(self, growth_bytes: int, *, poll_after_growth: bool) -> None:
        super().__init__(jarvis_bin="jarvis")
        self.growth_bytes = growth_bytes
        self.poll_after_growth = poll_after_growth

    def run_pipeline_streaming(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del (
            pipeline_path,
            on_stdout,
            on_stderr,
            on_start,
            should_cancel,
            timeout_seconds,
            on_timeout,
        )
        assert cwd is not None
        assert env is not None
        assert on_poll is not None
        Path(env["CLIO_RELAY_PROGRESS_FILE"]).write_text("", encoding="utf-8")
        Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"]).write_text("", encoding="utf-8")
        (cwd / "unbounded-child-output.bin").write_bytes(b"x" * self.growth_bytes)
        if self.poll_after_growth:
            on_poll()
            raise AssertionError("storage guard should stop the provider poll")
        return subprocess.CompletedProcess(["jarvis"], 0, "", "")


@pytest.mark.parametrize(
    "poll_after_growth",
    [True, False],
    ids=["interval-poll", "late-write-immediate-exit"],
)
def test_worker_fails_job_when_owned_child_exceeds_spool_reservation(
    tmp_path: Path,
    poll_after_growth: bool,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        spool_max_log_bytes_per_stream=1_000,
        spool_max_log_bytes_per_job=2_000,
    )
    limits = StorageLimits(
        core_high_water_bytes=1_000_000,
        spool_high_water_bytes=1_000_000,
        total_high_water_bytes=2_000_000,
        minimum_free_bytes=0,
        max_job_reservation_bytes=200_000,
        max_scan_entries=10_000,
        max_scan_depth=32,
        max_scan_accounted_bytes=2_000_000,
        max_ledger_bytes=1_000_000,
        max_reservations=100,
        lock_timeout_seconds=2,
    )
    runtime = StorageRuntime(
        StorageRuntimeConfig(
            core_root=settings.core_dir,
            spool_root=settings.spool_dir,
            max_log_bytes_per_job=settings.spool_max_log_bytes_per_job,
            job_core_allowance_bytes=1_000,
            job_result_allowance_bytes=1_000,
            runtime_check_interval_seconds=0.000_001,
            limits=limits,
        )
    )
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="configured-target",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["workload"]),
            idempotency_key="storage-growth-guard",
            storage_reservation=StorageReservationEstimate(
                core_bytes=10_000,
                spool_bytes=50_000,
            ),
        )
    )
    assert runtime.reconcile_startup(queue).allowed
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="configured-target",
        queue=queue,
        provider=_GrowingProvider(
            growth_bytes=60_000,
            poll_after_growth=poll_after_growth,
        ),
        storage_runtime=runtime,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    events, _cursor = queue.read_event_page(job.job_id, limit=100)
    guard_events = [event for event in events if event.event_type == "storage.runtime_guard_failed"]
    assert len(guard_events) == 1
    assert guard_events[0].payload["reason"] == "job_reservation_exceeded"
    assert runtime.policy.verify_reservation(
        job.job_id,
        core_bytes=10_000,
        spool_bytes=50_000,
    ).allowed


def test_worker_start_completes_legacy_queue_migration_before_registration(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    legacy = RelayJob(
        cluster="configured-target",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["legacy-workload"]),
        idempotency_key="legacy-worker-start",
    )
    jobs_dir = settings.core_dir / "jobs"
    jobs_dir.mkdir(parents=True)
    (jobs_dir / f"{legacy.job_id}.json").write_text(
        legacy.model_dump_json(indent=2),
        encoding="utf-8",
    )

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="configured-target",
        provider=_GrowingProvider(growth_bytes=0, poll_after_growth=False),
    )
    endpoint = worker.register()

    assert worker.queue.index_migration_status()["complete"] is True
    active, truncated = worker.queue.scan_active_jobs(limit=2)
    assert truncated is False
    assert [job.job_id for job in active] == [legacy.job_id]
    assert endpoint.cluster == "configured-target"
    assert worker.storage_runtime is not None
    assert worker.storage_runtime.status()["intake_allowed"] is True

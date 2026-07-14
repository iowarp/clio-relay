from __future__ import annotations

import sys
from pathlib import Path

import pytest

from clio_relay import process_containment
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.storage_policy import StorageLimits
from clio_relay.storage_runtime import (
    StorageRuntime,
    StorageRuntimeConfig,
    StorageRuntimeViolation,
)


def test_runtime_storage_violation_terminates_owned_child_tree(tmp_path: Path) -> None:
    core = tmp_path / "core"
    spool_root = tmp_path / "spool"
    runtime = StorageRuntime(
        StorageRuntimeConfig(
            core_root=core,
            spool_root=spool_root,
            max_log_bytes_per_job=100,
            job_core_allowance_bytes=100,
            job_result_allowance_bytes=100,
            runtime_check_interval_seconds=0.000_001,
            limits=StorageLimits(
                core_high_water_bytes=1_000_000,
                spool_high_water_bytes=1_000_000,
                total_high_water_bytes=2_000_000,
                minimum_free_bytes=0,
                max_job_reservation_bytes=100_000,
                max_scan_entries=10_000,
                max_scan_depth=32,
                max_scan_accounted_bytes=2_000_000,
                max_ledger_bytes=1_000_000,
                max_reservations=100,
                lock_timeout_seconds=2,
            ),
        )
    )
    job_id = "owned-child-storage-guard"
    spool = spool_root / job_id
    spool.mkdir()
    assert runtime.policy.reserve(job_id, core_bytes=1_000, spool_bytes=10_000).allowed
    provider = JarvisCdProvider(jarvis_bin="unused")
    process_ids: list[int] = []

    def guard() -> None:
        decision = runtime.check_running_job(job_id, spool_path=spool)
        if not decision.allowed:
            raise StorageRuntimeViolation(decision)

    with pytest.raises(StorageRuntimeViolation) as raised:
        provider.run_command_streaming(
            [
                sys.executable,
                "-c",
                (
                    "from pathlib import Path; import time; "
                    "Path('child-output.bin').write_bytes(b'x' * 20000); "
                    "time.sleep(60)"
                ),
            ],
            cwd=spool,
            on_start=process_ids.append,
            on_poll=guard,
        )

    assert raised.value.decision.reason.value == "job_reservation_exceeded"
    assert len(process_ids) == 1
    assert process_containment.process_start_identity(process_ids[0]) is None

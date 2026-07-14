"""Hard-crash fixture after storage reserve and core idempotency reservation."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from clio_relay.config import RelaySettings
from clio_relay.models import JarvisRunSpec, JobKind, RelayJob
from clio_relay.storage_runtime import storage_managed_queue


def _settings(root: Path) -> RelaySettings:
    return RelaySettings(
        core_dir=root / "core",
        spool_dir=root / "spool",
        spool_max_log_bytes_per_stream=50,
        spool_max_log_bytes_per_job=100,
        storage_core_high_water_bytes=1_000_000,
        storage_spool_high_water_bytes=1_000_000,
        storage_total_high_water_bytes=2_000_000,
        storage_minimum_free_bytes=0,
        storage_max_job_reservation_bytes=1_000,
        storage_max_scan_entries=10_000,
        storage_max_scan_depth=32,
        storage_max_scan_accounted_bytes=2_000_000,
        storage_max_ledger_bytes=1_000_000,
        storage_max_reservations=100,
        storage_lock_timeout_seconds=2,
        storage_job_core_allowance_bytes=20,
        storage_job_result_allowance_bytes=30,
        storage_runtime_check_interval_seconds=1,
    )


def main() -> None:
    root = Path(sys.argv[1])
    marker = Path(sys.argv[2])
    queue = storage_managed_queue(_settings(root))
    job = RelayJob(
        cluster="configured-target",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["workload"]),
        idempotency_key="hard-crash-storage-reserved",
    )
    marker.write_text(
        json.dumps({"job_id": job.job_id, "idempotency_key": job.idempotency_key}),
        encoding="utf-8",
    )

    def hard_crash(family: str, record_id: str) -> int:
        del family, record_id
        os._exit(91)

    queue._ensure_global_order_entry_unlocked = hard_crash  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    queue.submit_job(job)
    raise AssertionError("hard crash fault was not reached")


if __name__ == "__main__":
    main()

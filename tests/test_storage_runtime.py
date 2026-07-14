from __future__ import annotations

import json
from pathlib import Path

import pytest

import clio_relay.storage_policy as storage_module
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    RelayJob,
    StorageReservationEstimate,
)
from clio_relay.storage_policy import StorageLimits, StorageReason
from clio_relay.storage_runtime import (
    STORAGE_RUNTIME_STATUS_SCHEMA,
    StorageAdmissionError,
    StorageManagedQueue,
    StorageRuntime,
    StorageRuntimeConfig,
    storage_managed_queue,
    storage_runtime_from_settings,
)


def _config(tmp_path: Path, **changes: object) -> StorageRuntimeConfig:
    values: dict[str, object] = {
        "core_root": tmp_path / "core",
        "spool_root": tmp_path / "spool",
        "max_log_bytes_per_job": 100,
        "job_core_allowance_bytes": 20,
        "job_result_allowance_bytes": 30,
        "runtime_check_interval_seconds": 5.0,
        "limits": StorageLimits(
            core_high_water_bytes=1_000_000,
            spool_high_water_bytes=1_000_000,
            total_high_water_bytes=2_000_000,
            minimum_free_bytes=0,
            max_job_reservation_bytes=1_000,
            max_scan_entries=1_000,
            max_scan_depth=16,
            max_scan_accounted_bytes=2_000_000,
            max_ledger_bytes=1_000_000,
            max_reservations=100,
            lock_timeout_seconds=2,
        ),
    }
    values.update(changes)
    return StorageRuntimeConfig(**values)  # type: ignore[arg-type]


def _job(key: str, *, estimate: StorageReservationEstimate | None = None) -> RelayJob:
    return RelayJob(
        cluster="configured-target",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=key,
        storage_reservation=estimate,
    )


def test_runtime_reservation_sizing_covers_log_and_allowance_floors(tmp_path: Path) -> None:
    runtime = StorageRuntime(_config(tmp_path))

    default = runtime.estimate(_job("default"))
    explicit = runtime.estimate(
        _job(
            "explicit",
            estimate=StorageReservationEstimate(core_bytes=150, spool_bytes=170),
        )
    )

    assert default == StorageReservationEstimate(core_bytes=120, spool_bytes=130)
    assert explicit == StorageReservationEstimate(core_bytes=150, spool_bytes=170)


@pytest.mark.parametrize(
    ("estimate", "reason"),
    [
        (StorageReservationEstimate(core_bytes=119, spool_bytes=130), "invalid_request"),
        (StorageReservationEstimate(core_bytes=120, spool_bytes=129), "invalid_request"),
        (StorageReservationEstimate(core_bytes=500, spool_bytes=501), "per_job_limit"),
    ],
)
def test_runtime_rejects_underestimated_or_oversized_explicit_reservations(
    tmp_path: Path,
    estimate: StorageReservationEstimate,
    reason: str,
) -> None:
    runtime = StorageRuntime(_config(tmp_path))

    with pytest.raises(StorageAdmissionError) as raised:
        runtime.estimate(_job("invalid", estimate=estimate))

    payload = json.loads(str(raised.value))
    assert payload["reason"] == reason
    assert payload["allowed"] is False


def test_startup_reconcile_adopts_authoritative_nonterminal_jobs(tmp_path: Path) -> None:
    config = _config(tmp_path)
    queue = ClioCoreQueue(config.core_root)
    active = queue.submit_job(_job("active"))
    runtime = StorageRuntime(config)

    decision = runtime.reconcile_startup(queue)

    assert decision.allowed is True
    verified = runtime.policy.verify_reservation(
        active.job_id,
        core_bytes=120,
        spool_bytes=130,
    )
    assert verified.allowed is True
    status = runtime.status()
    assert status["schema"] == STORAGE_RUNTIME_STATUS_SCHEMA
    assert status["intake_allowed"] is True


def test_new_intake_is_closed_until_successful_startup_reconcile(tmp_path: Path) -> None:
    runtime = StorageRuntime(_config(tmp_path))

    with pytest.raises(StorageAdmissionError) as raised:
        runtime.ensure_new_intake_allowed()

    assert raised.value.decision.reason is StorageReason.INVALID_REQUEST
    queue = ClioCoreQueue(runtime.config.core_root)
    assert runtime.reconcile_startup(queue).allowed
    runtime.ensure_new_intake_allowed()


def test_running_guard_checks_free_space_each_poll_but_job_tree_on_interval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = StorageRuntime(_config(tmp_path))
    queue = ClioCoreQueue(runtime.config.core_root)
    job = queue.submit_job(_job("guard"))
    assert runtime.reconcile_startup(queue).allowed
    spool = runtime.config.spool_root / job.job_id
    spool.mkdir()
    scans = 0
    real_scan_tree = storage_module.scan_tree

    def counting_scan_tree(*args: object, **kwargs: object) -> object:
        nonlocal scans
        scans += 1
        return real_scan_tree(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(storage_module, "scan_tree", counting_scan_tree)

    first = runtime.check_running_job(job.job_id, spool_path=spool, now=10)
    second = runtime.check_running_job(job.job_id, spool_path=spool, now=11)
    third = runtime.check_running_job(job.job_id, spool_path=spool, now=15)

    assert first.allowed and second.allowed and third.allowed
    assert scans == 2
    runtime.forget_running_job(job.job_id)
    runtime.check_running_job(job.job_id, spool_path=spool, now=16)
    assert scans == 3


def test_runtime_config_refuses_defaults_larger_than_per_job_cap(tmp_path: Path) -> None:
    limits = StorageLimits(max_job_reservation_bytes=200)

    with pytest.raises(ValueError, match="exceeds max_job_reservation_bytes"):
        _config(tmp_path, limits=limits)


def test_storage_runtime_factory_consumes_validated_relay_settings(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        spool_max_log_bytes_per_job=100,
        storage_job_core_allowance_bytes=20,
        storage_job_result_allowance_bytes=30,
        storage_max_job_reservation_bytes=1_000,
        storage_runtime_check_interval_seconds=0.5,
    )

    runtime = storage_runtime_from_settings(settings)

    assert runtime.config.default_core_bytes == 120
    assert runtime.config.default_spool_bytes == 130
    assert runtime.config.runtime_check_interval_seconds == 0.5
    assert runtime.policy.core_root == settings.core_dir.absolute()
    assert runtime.policy.spool_root == settings.spool_dir.absolute()


def test_managed_queue_factory_completes_startup_and_opens_empty_intake(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")

    queue = storage_managed_queue(settings)

    assert isinstance(queue, StorageManagedQueue)
    assert queue.index_migration_status()["complete"] is True
    assert queue.storage_runtime.status()["intake_allowed"] is True

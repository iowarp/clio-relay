from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import clio_relay.storage_policy as storage_module
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    StorageReservationEstimate,
)
from clio_relay.storage_runtime import (
    StorageAdmissionError,
    StorageManagedQueue,
    storage_managed_queue,
)


def _settings(tmp_path: Path) -> RelaySettings:
    return RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
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


def _job(key: str) -> RelayJob:
    return RelayJob(
        cluster="configured-target",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["workload"]),
        idempotency_key=key,
    )


def _completed_outer_recovery(
    *,
    cluster: str,
    max_attempts: int = 3,
) -> list[RelayJob]:
    del cluster, max_attempts
    return []


def _resolve_as(
    queue: StorageManagedQueue,
    monkeypatch: pytest.MonkeyPatch,
    *,
    state: str,
    canonical_job_id: str,
    existing_job: RelayJob | None,
) -> None:
    def resolve(_job: RelayJob) -> SimpleNamespace:
        return SimpleNamespace(
            state=state,
            canonical_job_id=canonical_job_id,
            existing_job=existing_job,
        )

    monkeypatch.setattr(
        queue,
        "resolve_idempotent_submission",
        resolve,
        raising=False,
    )


def test_managed_queue_acquires_from_long_operator_configured_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Storage admission and queued-index scans share the safe internal root."""
    configured_root = tmp_path.joinpath(
        *(f"operator-storage-{index}-{'x' * 72}" for index in range(3))
    )
    queue = storage_managed_queue(_settings(configured_root))
    intent = _job("long-managed-queue")
    _resolve_as(
        queue,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )

    submitted = queue.submit_job(intent)
    lease = queue.acquire_next_job("long-managed-worker", cluster=intent.cluster)

    assert queue.root == configured_root / "core"
    assert lease is not None
    assert lease.job_id == submitted.job_id


def test_managed_queue_reserves_before_new_admission_and_releases_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = storage_managed_queue(_settings(tmp_path))
    intent = _job("reserve-release")
    _resolve_as(
        queue,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )

    saved = queue.submit_job(intent)
    verified = queue.storage_runtime.policy.verify_reservation(
        saved.job_id,
        core_bytes=120,
        spool_bytes=130,
    )
    terminal = queue.update_job_state(saved.job_id, JobState.SUCCEEDED)

    assert verified.allowed is True
    assert terminal.state is JobState.SUCCEEDED
    assert queue.storage_runtime.policy.release(saved.job_id).reason.value == ("reservation_absent")


def test_managed_queue_existing_replay_is_scan_free_under_new_pressure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path).model_copy(
        update={
            "storage_core_high_water_bytes": 50_000,
            "storage_spool_high_water_bytes": 50_000,
            "storage_total_high_water_bytes": 100_000,
        }
    )
    queue = storage_managed_queue(settings)
    intent = _job("idempotent-pressure")
    _resolve_as(
        queue,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )
    first = queue.submit_job(intent)
    (settings.core_dir / "pressure.bin").write_bytes(b"x" * 50_000)
    replay = _job("idempotent-pressure")
    _resolve_as(
        queue,
        monkeypatch,
        state="existing",
        canonical_job_id=first.job_id,
        existing_job=first,
    )

    def unexpected_scan(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("an existing idempotency replay must not scan storage trees")

    monkeypatch.setattr(storage_module, "scan_tree", unexpected_scan)
    repeated = queue.submit_job(replay)

    assert repeated.job_id == first.job_id


def test_managed_queue_never_scans_storage_while_core_lock_is_held(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = storage_managed_queue(_settings(tmp_path))
    intent = _job("lock-order")
    _resolve_as(
        queue,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )
    real_scan_tree = storage_module.scan_tree

    def checked_scan(*args: object, **kwargs: object) -> object:
        assert queue._lock.is_locked is False  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        return real_scan_tree(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(storage_module, "scan_tree", checked_scan)

    saved = queue.submit_job(intent)

    assert saved.job_id == intent.job_id


def test_managed_queue_releases_provisional_reservation_after_submit_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = storage_managed_queue(_settings(tmp_path))
    intent = _job("failed-submit")
    _resolve_as(
        queue,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )

    def fail_submit(_self: object, _job: RelayJob) -> RelayJob:
        raise RuntimeError("injected submit failure")

    monkeypatch.setattr("clio_relay.core_queue.ClioCoreQueue.submit_job", fail_submit)

    with pytest.raises(RuntimeError, match="injected submit failure"):
        queue.submit_job(intent)

    decision = queue.storage_runtime.policy.release(intent.job_id)
    assert decision.reason.value == "reservation_absent"


def test_managed_queue_denies_new_admission_with_stable_pressure_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path).model_copy(
        update={
            "storage_core_high_water_bytes": 1_000,
            "storage_spool_high_water_bytes": 1_000,
            "storage_total_high_water_bytes": 2_000,
        }
    )
    queue = storage_managed_queue(settings)
    (settings.core_dir / "pressure.bin").write_bytes(b"x" * 1_000)
    intent = _job("pressure-denial")
    _resolve_as(
        queue,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )

    with pytest.raises(StorageAdmissionError) as raised:
        queue.submit_job(intent)

    assert raised.value.decision.reason.value == "core_high_water"
    assert queue.storage_runtime.policy.release(intent.job_id).reason.value == (
        "reservation_absent"
    )


def test_managed_queue_releases_queued_cancellation_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = storage_managed_queue(_settings(tmp_path))
    intent = _job("queued-cancel")
    _resolve_as(
        queue,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )
    saved = queue.submit_job(intent)

    canceled, changed = queue.cancel_job_if_active(
        saved.job_id,
        cancel_scheduler=False,
    )

    assert changed is True
    assert canceled.state is JobState.CANCELED
    assert queue.storage_runtime.policy.release(saved.job_id).reason.value == ("reservation_absent")


def test_managed_queue_real_resolver_replays_without_second_reservation(
    tmp_path: Path,
) -> None:
    queue = storage_managed_queue(_settings(tmp_path))
    first = queue.submit_job(_job("real-idempotent-replay"))

    repeated = queue.submit_job(_job("real-idempotent-replay"))

    assert repeated.job_id == first.job_id
    status = queue.storage_runtime.policy.status().status
    assert status is not None
    assert status.reservation_count == 1


def test_explicit_storage_estimate_is_idempotency_significant(tmp_path: Path) -> None:
    queue = storage_managed_queue(_settings(tmp_path))
    first = _job("explicit-estimate").model_copy(
        update={
            "storage_reservation": StorageReservationEstimate(
                core_bytes=130,
                spool_bytes=140,
            )
        }
    )
    queue.submit_job(first)
    changed = _job("explicit-estimate").model_copy(
        update={
            "storage_reservation": StorageReservationEstimate(
                core_bytes=131,
                spool_bytes=140,
            )
        }
    )

    with pytest.raises(QueueConflictError, match="different or invalid job payload"):
        queue.submit_job(changed)

    status = queue.storage_runtime.policy.status().status
    assert status is not None
    assert status.reservation_count == 1


def test_managed_queue_recovers_crash_reserved_canonical_id_without_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = storage_managed_queue(_settings(tmp_path))
    original = _job("managed-crash-reserved")
    original_ensure = (
        queue._ensure_global_order_entry_unlocked  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )

    def fail_before_job_write(_family: str, _record_id: str) -> int:
        raise RuntimeError("fault after idempotency reservation")

    monkeypatch.setattr(queue, "_ensure_global_order_entry_unlocked", fail_before_job_write)
    with pytest.raises(RuntimeError, match="fault after idempotency reservation"):
        queue.submit_job(original)
    monkeypatch.setattr(queue, "_ensure_global_order_entry_unlocked", original_ensure)
    assert queue.storage_runtime.policy.release(original.job_id).reason.value == (
        "reservation_absent"
    )

    retry = _job("managed-crash-reserved")
    saved = queue.submit_job(retry)

    assert saved.job_id == original.job_id
    assert saved.job_id != retry.job_id
    status = queue.storage_runtime.policy.status().status
    assert status is not None
    assert status.reservation_count == 1


def test_managed_queue_stale_retry_exhaustion_releases_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = storage_managed_queue(_settings(tmp_path))
    intent = _job("stale-terminal")
    _resolve_as(
        queue,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )
    saved = queue.submit_job(intent)
    lease = queue.acquire_job(
        saved.job_id,
        "endpoint",
        cluster=saved.cluster,
        ttl_seconds=-1,
        max_attempts=1,
    )
    assert lease is not None

    recovered = queue.recover_stale_job(
        saved.job_id,
        cluster=saved.cluster,
        max_attempts=1,
    )

    assert recovered is not None
    assert recovered.state is JobState.FAILED
    assert queue.storage_runtime.policy.release(saved.job_id).reason.value == ("reservation_absent")


def test_managed_acquire_replays_stale_intent_created_after_outer_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    survivor = storage_managed_queue(settings)
    intent = _job("managed-stale-intent-race")
    _resolve_as(
        survivor,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )
    saved = survivor.submit_job(intent)
    old_lease = survivor.acquire_job(
        saved.job_id,
        "endpoint",
        cluster=saved.cluster,
        ttl_seconds=-1,
    )
    assert old_lease is not None

    crashing = ClioCoreQueue(settings.core_dir)

    def crash_after_job(_target: RelayJob, _leases: list[object]) -> None:
        raise RuntimeError("simulated concurrent stale-recovery crash")

    monkeypatch.setattr(crashing, "_after_stale_recovery_job_write", crash_after_job)
    with pytest.raises(RuntimeError, match="concurrent stale-recovery crash"):
        crashing.recover_stale_job(saved.job_id, cluster=saved.cluster)

    monkeypatch.setattr(survivor, "recover_stale_jobs", _completed_outer_recovery)
    replacement = survivor.acquire_job(
        saved.job_id,
        "endpoint",
        cluster=saved.cluster,
    )

    assert replacement is not None
    assert replacement.lease_id != old_lease.lease_id
    assert list((settings.core_dir / "transition_intents").glob("*.json")) == []
    leases, truncated = survivor.scan_job_leases(saved.job_id, limit=10)
    assert truncated is False
    assert [lease.lease_id for lease in leases] == [replacement.lease_id]


def test_managed_acquire_replay_releases_terminal_stale_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    survivor = storage_managed_queue(settings)
    intent = _job("managed-terminal-stale-intent-race")
    _resolve_as(
        survivor,
        monkeypatch,
        state="new",
        canonical_job_id=intent.job_id,
        existing_job=None,
    )
    saved = survivor.submit_job(intent)
    assert (
        survivor.acquire_job(
            saved.job_id,
            "endpoint",
            cluster=saved.cluster,
            ttl_seconds=-1,
            max_attempts=1,
        )
        is not None
    )
    crashing = ClioCoreQueue(settings.core_dir)

    def crash_after_job(_target: RelayJob, _leases: list[object]) -> None:
        raise RuntimeError("simulated terminal stale-recovery crash")

    monkeypatch.setattr(crashing, "_after_stale_recovery_job_write", crash_after_job)
    with pytest.raises(RuntimeError, match="terminal stale-recovery crash"):
        crashing.recover_stale_job(
            saved.job_id,
            cluster=saved.cluster,
            max_attempts=1,
        )

    monkeypatch.setattr(survivor, "recover_stale_jobs", _completed_outer_recovery)
    assert (
        survivor.acquire_job(
            saved.job_id,
            "endpoint",
            cluster=saved.cluster,
            max_attempts=1,
        )
        is None
    )
    assert survivor.get_job(saved.job_id).state is JobState.FAILED
    assert survivor.storage_runtime.policy.release(saved.job_id).reason.value == (
        "reservation_absent"
    )


def test_hard_crash_reservation_is_reconciled_before_canonical_retry(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "hard-crash-storage.json"
    crashed = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.storage_crash_fixture",
            str(tmp_path),
            str(marker),
        ],
        cwd=Path(__file__).parents[1],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert crashed.returncode == 91, crashed.stderr
    identity = json.loads(marker.read_text(encoding="utf-8"))
    original_job_id = identity["job_id"]
    assert isinstance(original_job_id, str)

    queue = storage_managed_queue(_settings(tmp_path))
    before_retry = queue.storage_runtime.policy.status().status
    assert before_retry is not None
    assert before_retry.reservation_count == 0
    retry = _job("hard-crash-storage-reserved")
    saved = queue.submit_job(retry)

    assert saved.job_id == original_job_id
    assert saved.job_id != retry.job_id
    after_retry = queue.storage_runtime.policy.status().status
    assert after_retry is not None
    assert after_retry.reservation_count == 1

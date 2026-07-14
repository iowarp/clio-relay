from __future__ import annotations

import json
import os
import stat
import subprocess
from collections.abc import Generator, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import clio_relay.storage_policy as storage_module
from clio_relay.filesystem_paths import logical_filesystem_path
from clio_relay.storage_policy import (
    StorageLimits,
    StoragePolicy,
    StoragePolicyError,
    StorageReason,
    scan_tree,
)


def _limits(**changes: object) -> StorageLimits:
    base = StorageLimits(
        core_high_water_bytes=10_000_000,
        spool_high_water_bytes=10_000_000,
        total_high_water_bytes=20_000_000,
        minimum_free_bytes=0,
        max_job_reservation_bytes=5_000_000,
        max_scan_entries=10_000,
        max_scan_depth=32,
        max_scan_accounted_bytes=100_000_000,
        max_ledger_bytes=1_000_000,
        max_reservations=1_000,
        lock_timeout_seconds=2,
    )
    return replace(base, **changes)


def _policy(
    tmp_path: Path,
    *,
    limits: StorageLimits | None = None,
    clock: object | None = None,
) -> StoragePolicy:
    core = tmp_path / "core"
    spool = tmp_path / "spool"
    core.mkdir(exist_ok=True)
    spool.mkdir(exist_ok=True)
    if clock is None:
        return StoragePolicy(
            core,
            spool,
            state_root=tmp_path / "policy-state",
            limits=limits or _limits(),
        )
    return StoragePolicy(
        core,
        spool,
        state_root=tmp_path / "policy-state",
        limits=limits or _limits(),
        clock=clock,  # type: ignore[arg-type]
    )


def _make_directory_link(target: Path, link: Path) -> None:
    try:
        os.symlink(target, link, target_is_directory=True)
        return
    except OSError:
        if os.name != "nt":
            raise
    completed = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f"could not create test junction: {completed.stderr}")


def test_status_is_bounded_machine_readable_and_target_agnostic(tmp_path: Path) -> None:
    policy = _policy(tmp_path)

    decision = policy.status()
    payload = decision.to_dict()

    assert decision.allowed is True
    assert decision.reason is StorageReason.HEALTHY
    assert payload["schema"] == "clio-relay.storage-decision.v1"
    status = payload["status"]
    assert isinstance(status, dict)
    assert status["schema"] == "clio-relay.storage-status.v1"
    assert status["reservation_count"] == 0
    assert "cluster" not in json.dumps(payload).lower()
    assert "target" not in json.dumps(payload).lower()


def test_storage_snapshot_retries_transient_tree_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _policy(tmp_path)
    real_scan_tree = storage_module.scan_tree
    calls = 0
    sleeps: list[float] = []

    def transient_scan(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise StoragePolicyError(
                StorageReason.SCAN_CHANGED,
                "storage tree changed while it was being accounted",
            )
        return real_scan_tree(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(storage_module, "scan_tree", transient_scan)
    monkeypatch.setattr(storage_module.time, "sleep", sleeps.append)

    decision = policy.status()

    assert decision.allowed is True
    assert calls == 4
    assert sleeps == [
        storage_module.STORAGE_SNAPSHOT_SCAN_RETRY_SECONDS,
        storage_module.STORAGE_SNAPSHOT_SCAN_RETRY_SECONDS,
    ]


def test_storage_snapshot_persistent_churn_fails_closed_after_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _policy(tmp_path)
    calls = 0

    def changing_scan(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise StoragePolicyError(
            StorageReason.SCAN_CHANGED,
            "storage tree changed while it was being accounted",
        )

    def no_sleep(_seconds: float) -> None:
        return

    monkeypatch.setattr(storage_module, "scan_tree", changing_scan)
    monkeypatch.setattr(storage_module.time, "sleep", no_sleep)

    decision = policy.status()

    assert decision.allowed is False
    assert decision.reason is StorageReason.SCAN_CHANGED
    assert calls == storage_module.STORAGE_SNAPSHOT_SCAN_ATTEMPTS


def test_limits_require_runtime_scan_to_cover_every_legal_job_reservation() -> None:
    with pytest.raises(
        ValueError,
        match="max_scan_accounted_bytes must be at least max_job_reservation_bytes",
    ):
        StorageLimits(
            max_job_reservation_bytes=1_001,
            max_scan_accounted_bytes=1_000,
        )


def test_scan_counts_hardlinks_per_entry_conservatively(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    original = root / "original"
    original.write_bytes(b"1234567")
    os.link(original, root / "alias")

    usage = scan_tree(
        root,
        max_entries=10,
        max_depth=4,
        max_accounted_bytes=100,
    )

    assert usage.bytes == 14
    assert usage.files == 2
    assert usage.links == 0


def test_core_rejects_links_without_traversing_them(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "large").write_bytes(b"x" * 50_000)
    _make_directory_link(outside, policy.core_root / "external")

    decision = policy.status()

    assert decision.allowed is False
    assert decision.reason is StorageReason.SCAN_UNSAFE_ENTRY


def test_spool_counts_link_entry_but_never_target_tree(tmp_path: Path) -> None:
    policy = _policy(tmp_path, limits=_limits(spool_high_water_bytes=1000))
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "large").write_bytes(b"x" * 50_000)
    link = policy.spool_root / "output-link"
    _make_directory_link(outside, link)

    decision = policy.status()

    assert decision.allowed is True
    assert decision.status is not None
    assert decision.status.spool.links == 1
    assert decision.status.spool.bytes == max(0, os.lstat(link).st_size)


def test_scan_fails_closed_at_entry_depth_and_byte_bounds(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    (root / "one").write_bytes(b"1")
    (root / "two").write_bytes(b"2")
    with pytest.raises(StoragePolicyError) as entries:
        scan_tree(root, max_entries=1, max_depth=4, max_accounted_bytes=100)
    assert entries.value.reason is StorageReason.SCAN_ENTRY_LIMIT

    nested = root / "a"
    nested.mkdir()
    (nested / "b").mkdir()
    with pytest.raises(StoragePolicyError) as depth:
        scan_tree(root, max_entries=10, max_depth=1, max_accounted_bytes=100)
    assert depth.value.reason is StorageReason.SCAN_DEPTH_LIMIT

    with pytest.raises(StoragePolicyError) as byte_limit:
        scan_tree(root, max_entries=10, max_depth=4, max_accounted_bytes=1)
    assert byte_limit.value.reason is StorageReason.SCAN_BYTE_LIMIT


def test_scan_rejects_nonregular_entries_or_static_reparse_marker(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    if os.name != "nt":
        os.mkfifo(root / "fifo")
        with pytest.raises(StoragePolicyError) as raised:
            scan_tree(root, max_entries=10, max_depth=4, max_accounted_bytes=100)
        assert raised.value.reason is StorageReason.SCAN_UNSAFE_ENTRY
    else:
        outside = tmp_path / "outside"
        outside.mkdir()
        _make_directory_link(outside, root / "junction")
        with pytest.raises(StoragePolicyError) as raised:
            scan_tree(root, max_entries=10, max_depth=4, max_accounted_bytes=100)
        assert raised.value.reason is StorageReason.SCAN_UNSAFE_ENTRY


def test_reserve_is_durable_idempotent_and_conflicts_on_resize(tmp_path: Path) -> None:
    fixed = datetime(2026, 7, 11, 12, 30, tzinfo=UTC)
    policy = _policy(tmp_path, clock=lambda: fixed)

    created = policy.reserve("job-1", core_bytes=100, spool_bytes=200)
    repeated = policy.reserve("job-1", core_bytes=100, spool_bytes=200)
    conflict = policy.reserve("job-1", core_bytes=101, spool_bytes=200)
    reopened = _policy(tmp_path)

    assert created.reason is StorageReason.RESERVED
    assert repeated.reason is StorageReason.RESERVATION_IDEMPOTENT
    assert conflict.allowed is False
    assert conflict.reason is StorageReason.RESERVATION_CONFLICT
    status = reopened.status()
    assert status.status is not None
    assert status.status.reservation_count == 1
    assert status.status.reserved_core_bytes == 100
    assert status.status.reserved_spool_bytes == 200
    ledger = json.loads(policy.ledger_path.read_text(encoding="utf-8"))
    assert ledger["reservations"][0]["created_at"] == "2026-07-11T12:30:00Z"
    assert ledger["checksum"].startswith("sha256:")


def test_existing_idempotent_reservation_is_denied_under_new_pressure(tmp_path: Path) -> None:
    policy = _policy(
        tmp_path,
        limits=_limits(
            core_high_water_bytes=100,
            spool_high_water_bytes=100,
            total_high_water_bytes=200,
        ),
    )
    assert policy.reserve("job-1", core_bytes=10, spool_bytes=1).allowed
    (policy.core_root / "pressure").write_bytes(b"x" * 100)

    repeated = policy.reserve("job-1", core_bytes=10, spool_bytes=1)

    assert repeated.allowed is False
    assert repeated.reason is StorageReason.CORE_HIGH_WATER
    assert dict(repeated.details or {})["idempotent"] is True


@pytest.mark.parametrize(
    ("limits", "core_bytes", "spool_bytes", "reason"),
    [
        (
            _limits(
                core_high_water_bytes=100,
                spool_high_water_bytes=200,
                total_high_water_bytes=300,
            ),
            101,
            0,
            StorageReason.CORE_HIGH_WATER,
        ),
        (
            _limits(
                core_high_water_bytes=200,
                spool_high_water_bytes=100,
                total_high_water_bytes=300,
            ),
            0,
            101,
            StorageReason.SPOOL_HIGH_WATER,
        ),
        (
            _limits(
                core_high_water_bytes=100,
                spool_high_water_bytes=100,
                total_high_water_bytes=150,
            ),
            80,
            80,
            StorageReason.TOTAL_HIGH_WATER,
        ),
    ],
)
def test_reserve_enforces_family_and_combined_high_water(
    tmp_path: Path,
    limits: StorageLimits,
    core_bytes: int,
    spool_bytes: int,
    reason: StorageReason,
) -> None:
    decision = _policy(tmp_path, limits=limits).reserve(
        "job-limit", core_bytes=core_bytes, spool_bytes=spool_bytes
    )

    assert decision.allowed is False
    assert decision.reason is reason


def test_same_volume_free_space_is_queried_and_reserved_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[Path] = []

    def disk_usage(path: Path) -> SimpleNamespace:
        calls.append(path)
        return SimpleNamespace(total=1000, used=900, free=100)

    monkeypatch.setattr(storage_module.shutil, "disk_usage", disk_usage)
    policy = _policy(tmp_path, limits=_limits(minimum_free_bytes=51))

    decision = policy.reserve("job-space", core_bytes=30, spool_bytes=20)

    assert decision.allowed is False
    assert decision.reason is StorageReason.FILESYSTEM_FREE_RESERVE
    assert len(calls) == 1
    assert decision.status is not None
    assert len(decision.status.volumes) == 1
    volume = decision.status.volumes[0]
    assert volume.storage_families == ("core", "spool")
    assert volume.reserved_bytes == 50
    assert volume.available_after_reservations_bytes == 50


def test_release_and_crash_reconcile_are_idempotent(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    assert policy.reserve("active", core_bytes=1, spool_bytes=1).allowed
    assert policy.reserve("crash-stale", core_bytes=2, spool_bytes=2).allowed

    reconciled = policy.reconcile(["active"])
    repeated = policy.reconcile(["active"])
    released = policy.release("active")
    absent = policy.release("active")

    assert reconciled.reason is StorageReason.RECONCILED
    assert dict(reconciled.details or {})["released_job_ids"] == ["crash-stale"]
    first_generation = dict(reconciled.details or {})["ledger_generation"]
    assert dict(repeated.details or {})["ledger_generation"] == first_generation
    assert released.reason is StorageReason.RESERVATION_RELEASED
    assert absent.reason is StorageReason.RESERVATION_ABSENT
    assert policy.status().status is not None
    assert policy.status().status.reservation_count == 0  # type: ignore[union-attr]


def test_reservation_ledger_capacity_and_per_job_limit_fail_closed(tmp_path: Path) -> None:
    policy = _policy(
        tmp_path,
        limits=_limits(max_reservations=1, max_job_reservation_bytes=10),
    )

    too_large = policy.reserve("large", core_bytes=11, spool_bytes=0)
    first = policy.reserve("first", core_bytes=5, spool_bytes=0)
    capacity = policy.reserve("second", core_bytes=5, spool_bytes=0)

    assert too_large.reason is StorageReason.PER_JOB_LIMIT
    assert first.allowed is True
    assert capacity.reason is StorageReason.LEDGER_CAPACITY


@pytest.mark.parametrize(
    ("job_id", "core_bytes", "spool_bytes"),
    [
        ("../escape", 1, 0),
        ("", 1, 0),
        ("job", -1, 0),
        ("job", 0, 0),
    ],
)
def test_invalid_reservations_are_machine_readable(
    tmp_path: Path, job_id: str, core_bytes: int, spool_bytes: int
) -> None:
    decision = _policy(tmp_path).reserve(job_id, core_bytes=core_bytes, spool_bytes=spool_bytes)

    assert decision.allowed is False
    assert decision.reason is StorageReason.INVALID_REQUEST


def test_malformed_and_checksum_modified_ledgers_deny_without_replacement(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    policy.ledger_path.write_text("{not json", encoding="utf-8")
    os.chmod(policy.ledger_path, 0o600)
    before = policy.ledger_path.read_bytes()

    malformed = policy.reserve("job", core_bytes=1, spool_bytes=0)

    assert malformed.reason is StorageReason.LEDGER_MALFORMED
    assert policy.ledger_path.read_bytes() == before

    policy.ledger_path.unlink()
    assert policy.reserve("valid", core_bytes=1, spool_bytes=0).allowed
    envelope = json.loads(policy.ledger_path.read_text(encoding="utf-8"))
    envelope["generation"] += 1
    policy.ledger_path.write_text(json.dumps(envelope), encoding="utf-8")
    os.chmod(policy.ledger_path, 0o600)

    corrupted = policy.status()
    assert corrupted.allowed is False
    assert corrupted.reason is StorageReason.LEDGER_MALFORMED


def test_oversized_ledger_is_read_with_a_hard_max_plus_one_bound(tmp_path: Path) -> None:
    policy = _policy(tmp_path, limits=_limits(max_ledger_bytes=128))
    policy.ledger_path.write_bytes(b"x" * 129)
    os.chmod(policy.ledger_path, 0o600)

    decision = policy.status()

    assert decision.allowed is False
    assert decision.reason is StorageReason.LEDGER_OVERSIZED


def test_dangling_or_linked_ledger_is_not_treated_as_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = _policy(tmp_path)
    real_lstat = storage_module.os.lstat
    link_stat = os.stat_result((stat.S_IFLNK | 0o777, 0, 0, 1, 0, 0, 8, 0, 0, 0))

    def lstat(path: os.PathLike[str] | str) -> os.stat_result:
        if logical_filesystem_path(Path(path)) == policy.ledger_path:
            return link_stat
        return real_lstat(path)

    monkeypatch.setattr(storage_module.os, "lstat", lstat)

    decision = policy.status()

    assert decision.allowed is False
    assert decision.reason is StorageReason.LEDGER_UNSAFE


def test_hardlinked_ledger_and_lock_files_are_rejected(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    assert policy.reserve("job", core_bytes=1, spool_bytes=0).allowed
    os.link(policy.ledger_path, tmp_path / "ledger-alias")

    ledger_decision = policy.status()

    assert ledger_decision.reason is StorageReason.LEDGER_UNSAFE

    (tmp_path / "ledger-alias").unlink()
    policy.lock_path.write_bytes(b"")
    os.link(policy.lock_path, tmp_path / "lock-alias")
    lock_decision = policy.status()
    assert lock_decision.reason is StorageReason.LEDGER_UNSAFE


def test_atomic_replace_failure_preserves_previous_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = _policy(tmp_path)
    assert policy.reserve("first", core_bytes=1, spool_bytes=0).allowed
    original = policy.ledger_path.read_bytes()

    def fail_replace(_source: Path, _destination: Path) -> None:
        raise OSError("injected replace failure")

    monkeypatch.setattr(storage_module, "_replace_file", fail_replace)
    failed = policy.reserve("second", core_bytes=1, spool_bytes=0)

    assert failed.reason is StorageReason.PERSISTENCE_FAILURE
    assert policy.ledger_path.read_bytes() == original
    assert not list(policy.state_root.glob("*.tmp"))


def test_windows_replace_retries_sharing_violations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = _policy(tmp_path)
    real_replace = storage_module.os.replace
    calls = 0

    def flaky_replace(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise PermissionError("injected scanner sharing violation")
        real_replace(source, destination)

    monkeypatch.setattr(storage_module.os, "replace", flaky_replace)
    decision = policy.reserve("retry", core_bytes=1, spool_bytes=0)

    if os.name == "nt":
        assert decision.allowed is True
        assert calls == 3
    else:
        assert decision.reason is StorageReason.PERSISTENCE_FAILURE
        assert calls == 1


def test_orphan_temporary_file_does_not_override_committed_ledger(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    assert policy.reserve("committed", core_bytes=3, spool_bytes=4).allowed
    (policy.state_root / ".reservations.v1.orphan.tmp").write_text(
        "invalid partial bytes", encoding="utf-8"
    )

    reopened = _policy(tmp_path).status()

    assert reopened.allowed is True
    assert reopened.status is not None
    assert reopened.status.reservation_count == 1
    assert reopened.status.reserved_core_bytes == 3


def test_concurrent_reservations_never_overcommit_high_water(tmp_path: Path) -> None:
    limits = _limits(
        core_high_water_bytes=100,
        spool_high_water_bytes=100,
        total_high_water_bytes=200,
    )
    policies = [_policy(tmp_path, limits=limits) for _ in range(20)]

    def reserve(index: int) -> bool:
        return policies[index].reserve(f"job-{index:02d}", core_bytes=10, spool_bytes=0).allowed

    with ThreadPoolExecutor(max_workers=8) as executor:
        accepted = list(executor.map(reserve, range(20)))

    status = _policy(tmp_path, limits=limits).status()
    assert sum(accepted) == 10
    assert status.status is not None
    assert status.status.reservation_count == 10
    assert status.status.reserved_core_bytes == 100


def test_scan_identity_race_denies_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = _policy(tmp_path)

    @contextmanager
    def changed_scan(
        _path: Path, _expected_device: int, _expected_inode: int
    ) -> Generator[Iterator[os.DirEntry[str]], None, None]:
        raise FileNotFoundError("injected rename race")
        yield iter(())

    monkeypatch.setattr(storage_module, "_scandir_verified", changed_scan)

    decision = policy.reserve("job-race", core_bytes=1, spool_bytes=0)

    assert decision.allowed is False
    assert decision.reason is StorageReason.SCAN_CHANGED
    assert not policy.ledger_path.exists()


def test_state_root_is_private_or_unsafe_permissions_fail_explicitly(tmp_path: Path) -> None:
    core = tmp_path / "core"
    spool = tmp_path / "spool"
    core.mkdir()
    spool.mkdir()
    state_root = tmp_path / "state"
    state_root.mkdir()
    if os.name != "nt":
        os.chmod(state_root, 0o777)
        with pytest.raises(StoragePolicyError) as raised:
            StoragePolicy(core, spool, state_root=state_root, limits=_limits())
        assert raised.value.reason is StorageReason.LEDGER_UNSAFE
    else:
        policy = StoragePolicy(core, spool, state_root=state_root, limits=_limits())
        assert policy.state_root.is_dir()


def test_created_state_and_ledger_are_owner_private_on_posix(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    assert policy.reserve("job", core_bytes=1, spool_bytes=0).allowed

    if os.name != "nt":
        assert stat.S_IMODE(os.lstat(policy.state_root).st_mode) == 0o700
        assert stat.S_IMODE(os.lstat(policy.ledger_path).st_mode) == 0o600
        assert stat.S_IMODE(os.lstat(policy.lock_path).st_mode) == 0o600
    else:
        assert stat.S_ISREG(os.lstat(policy.ledger_path).st_mode)
        assert os.lstat(policy.ledger_path).st_nlink == 1


def test_roots_must_not_overlap(tmp_path: Path) -> None:
    core = tmp_path / "data"
    spool = core / "spool"
    spool.mkdir(parents=True)

    with pytest.raises(ValueError, match="non-nested"):
        StoragePolicy(core, spool, state_root=tmp_path / "state", limits=_limits())


def test_reconcile_reservations_adopts_active_and_releases_stale_in_one_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _policy(tmp_path)
    assert policy.reserve("stale", core_bytes=3, spool_bytes=4).allowed
    scans = 0
    real_scan_tree = storage_module.scan_tree

    def counting_scan_tree(*args: object, **kwargs: object) -> object:
        nonlocal scans
        scans += 1
        return real_scan_tree(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(storage_module, "scan_tree", counting_scan_tree)
    decision = policy.reconcile_reservations(
        {
            "active-a": (10, 20),
            "active-b": (30, 40),
        }
    )

    assert decision.allowed is True
    assert decision.reason is StorageReason.RECONCILED
    assert scans == 2
    assert dict(decision.details or {})["adopted_job_ids"] == ["active-a", "active-b"]
    assert dict(decision.details or {})["released_job_ids"] == ["stale"]
    status = policy.status().status
    assert status is not None
    assert status.reservation_count == 2
    assert status.reserved_core_bytes == 40
    assert status.reserved_spool_bytes == 60


def test_reconcile_records_authoritative_active_set_even_under_pressure(tmp_path: Path) -> None:
    policy = _policy(
        tmp_path,
        limits=_limits(
            core_high_water_bytes=10,
            spool_high_water_bytes=10,
            total_high_water_bytes=20,
        ),
    )

    decision = policy.reconcile_reservations({"active": (11, 1)})

    assert decision.allowed is False
    assert decision.reason is StorageReason.CORE_HIGH_WATER
    assert dict(decision.details or {})["persisted"] is True
    ledger = json.loads(policy.ledger_path.read_text(encoding="utf-8"))
    assert [item["job_id"] for item in ledger["reservations"]] == ["active"]


def test_reconcile_refuses_to_resize_active_reservation(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    assert policy.reserve("active", core_bytes=10, spool_bytes=20).allowed

    conflict = policy.reconcile_reservations({"active": (10, 21)})

    assert conflict.allowed is False
    assert conflict.reason is StorageReason.RESERVATION_CONFLICT
    verified = policy.verify_reservation("active", core_bytes=10, spool_bytes=20)
    assert verified.allowed is True


def test_verify_idempotent_reservation_does_not_rescan_or_deny_new_pressure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _policy(
        tmp_path,
        limits=_limits(
            core_high_water_bytes=100,
            spool_high_water_bytes=100,
            total_high_water_bytes=200,
        ),
    )
    assert policy.reserve("active", core_bytes=10, spool_bytes=20).allowed
    (policy.core_root / "pressure").write_bytes(b"x" * 100)

    def unexpected_scan(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("idempotency verification must not scan storage trees")

    monkeypatch.setattr(storage_module, "scan_tree", unexpected_scan)
    verified = policy.verify_reservation("active", core_bytes=10, spool_bytes=20)

    assert verified.allowed is True
    assert verified.reason is StorageReason.RESERVATION_IDEMPOTENT


def test_runtime_guard_checks_only_job_spool_and_enforces_reservation(
    tmp_path: Path,
) -> None:
    policy = _policy(tmp_path)
    spool = policy.spool_root / "active"
    spool.mkdir()
    (spool / "output").write_bytes(b"1234")
    assert policy.reserve("active", core_bytes=10, spool_bytes=4).allowed

    healthy = policy.check_runtime_job("active", spool_path=spool)
    (spool / "overflow").write_bytes(b"5")
    exceeded = policy.check_runtime_job("active", spool_path=spool)

    assert healthy.allowed is True
    assert healthy.reason is StorageReason.HEALTHY
    assert exceeded.allowed is False
    assert exceeded.reason is StorageReason.JOB_RESERVATION_EXCEEDED


def test_runtime_guard_fails_closed_for_missing_reservation_and_free_space(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _policy(tmp_path, limits=_limits(minimum_free_bytes=101))
    spool = policy.spool_root / "active"
    spool.mkdir()

    missing = policy.check_runtime_job("active", spool_path=spool)
    assert missing.reason is StorageReason.RESERVATION_ABSENT
    assert policy.reserve("active", core_bytes=1, spool_bytes=1).allowed

    pressure_root = tmp_path / "pressure"
    pressure_root.mkdir()
    pressure_policy = _policy(pressure_root, limits=_limits(minimum_free_bytes=101))
    pressure_spool = pressure_policy.spool_root / "active"
    pressure_spool.mkdir()

    def ample_space(_path: Path) -> SimpleNamespace:
        return SimpleNamespace(total=1000, used=0, free=1000)

    monkeypatch.setattr(
        storage_module.shutil,
        "disk_usage",
        ample_space,
    )
    assert pressure_policy.reserve("active", core_bytes=1, spool_bytes=1).allowed

    def low_space(_path: Path) -> SimpleNamespace:
        return SimpleNamespace(total=1000, used=900, free=100)

    monkeypatch.setattr(
        storage_module.shutil,
        "disk_usage",
        low_space,
    )

    pressure = pressure_policy.check_runtime_job("active", spool_path=pressure_spool)
    assert pressure.allowed is False
    assert pressure.reason is StorageReason.FILESYSTEM_FREE_RESERVE


def test_runtime_free_space_guard_is_constant_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _policy(tmp_path)

    def unexpected_scan(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("free-space guard must not scan a storage tree")

    monkeypatch.setattr(storage_module, "scan_tree", unexpected_scan)

    decision = policy.check_runtime_free_space()

    assert decision.allowed is True
    assert decision.reason is StorageReason.HEALTHY

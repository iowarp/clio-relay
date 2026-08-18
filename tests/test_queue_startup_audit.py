"""Indexed-era queue startup and bounded legacy-audit recovery tests."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn

import pytest

from clio_relay import (
    queue_legacy_audit,
    queue_legacy_output_audit,
    queue_startup,
    worker_lifetime_lock,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import JarvisRunSpec, JobKind, RelayJob
from clio_relay.queue_store_lock import LegacyQueueStateError
from clio_relay.worker_lifetime_lock import (
    LockedCoreIdentity,
    WorkerLifetimeLock,
    WorkerLifetimeLockUnavailable,
)


def _job(identity: str) -> RelayJob:
    return RelayJob(
        job_id=identity,
        cluster="configured-target",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=f"submit-{identity}",
    )


def _audit_marker(root: Path) -> Path:
    return root / "migrations" / "legacy-record-audit-v1.json"


def _refuse_history_scan(*_args: object, **_kwargs: object) -> NoReturn:
    raise AssertionError("indexed-era startup must not enumerate canonical history")


def _no_audit_fault(_phase: str, _path: Path) -> None:
    return


def test_indexed_era_fresh_process_startup_does_not_scan_record_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durable seal makes every later process independent of record count."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    for index in range(4):
        queue.submit_job(_job(f"job_indexed_{index}"))

    monkeypatch.setattr(
        queue_legacy_audit,
        "audit_before_initialization",
        _refuse_history_scan,
    )
    monkeypatch.setattr(
        ClioCoreQueue,
        "_audit_completed_legacy_output_state",
        _refuse_history_scan,
    )
    monkeypatch.setattr(
        ClioCoreQueue,
        "_bounded_legacy_family_entries",
        _refuse_history_scan,
    )
    monkeypatch.setattr(
        queue_legacy_output_audit.queue_legacy_output_codec,
        "iter_legacy_event_paths",
        _refuse_history_scan,
    )
    monkeypatch.setattr(queue_legacy_audit.queue_layout, "MAX_BOUNDED_SCAN_RECORDS", 1)

    reopened = ClioCoreQueue(root)
    reopened.initialize()

    assert reopened.get_job("job_indexed_3").job_id == "job_indexed_3"


def test_sealed_startup_never_uses_path_glob(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The healthy sealed path has no directory-history-shaped glob probe."""
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    monkeypatch.setattr(Path, "glob", _refuse_history_scan)

    ClioCoreQueue(root).initialize()


def test_sealed_startup_never_upgrades_shared_writer_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A healthy seal retains the bounded shared-writer startup path."""
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    isolated_worker_lifetime_lock = SimpleNamespace(
        **{**vars(worker_lifetime_lock), "exclusive_migration_lifetime": _refuse_history_scan}
    )
    monkeypatch.setattr(queue_startup, "worker_lifetime_lock", isolated_worker_lifetime_lock)

    ClioCoreQueue(root).initialize()


def test_missing_seal_is_written_once_while_exclusive_writer_ownership_is_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fresh initialization binds its one durable seal to exclusive writer fencing."""
    root = tmp_path / "core"
    sealed_phases: list[str] = []

    def observe_seal(phase: str, _path: Path) -> None:
        with pytest.raises(WorkerLifetimeLockUnavailable):
            WorkerLifetimeLock(
                root,
                mode="shared",
                timeout_seconds=0,
            ).acquire()
        sealed_phases.append(phase)

    monkeypatch.setattr(
        queue_legacy_audit,
        "after_audit_phase",
        staticmethod(observe_seal),
    )

    ClioCoreQueue(root).initialize()

    assert sealed_phases == ["marker"]


def test_reentrant_initialize_dispatch_honors_a_subclass_override(
    tmp_path: Path,
) -> None:
    """Both re-entrant ``initialize`` call sites must virtually dispatch
    through the runtime type, not call the bare module function directly
    (F4, closing-round review). A fresh root's missing-seal path re-enters
    ``initialize`` twice more internally (once with ``locked_core`` set from
    ``_initialize_with_exclusive_lifetime``, once more from
    ``_initialize_under_locked_core``); a de-virtualized re-entry would
    silently skip a subclass override exactly as it silently skipped
    ``StorageManagedQueue.initialize``'s closed-guard and forced
    ``allow_exclusive_seal=False`` in production."""
    calls: list[LockedCoreIdentity | None] = []

    class _OverridingQueue(ClioCoreQueue):
        def initialize(
            self,
            *,
            migrate_legacy_output: bool = False,
            locked_core: LockedCoreIdentity | None = None,
            allow_exclusive_seal: bool = True,
        ) -> None:
            calls.append(locked_core)
            super().initialize(
                migrate_legacy_output=migrate_legacy_output,
                locked_core=locked_core,
                allow_exclusive_seal=allow_exclusive_seal,
            )

    queue = _OverridingQueue(tmp_path / "core")
    queue.initialize()

    # entries: outer call (locked_core=None), the exclusive-lifetime
    # re-entry (locked_core set), and the locked-core re-entry (locked_core
    # cleared again once the swapped root is authoritative).
    assert len(calls) == 3
    assert calls[0] is None
    assert calls[1] is not None
    assert calls[2] is None


def test_sealed_startup_refuses_malformed_index_state_without_repair_or_glob(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A seal turns its fixed migration-state shape into immutable startup evidence."""
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    index_path = root / "migrations" / "index-v1.json"
    state = json.loads(index_path.read_bytes())
    del state["order_families"]
    payload = json.dumps(state).encode()
    index_path.write_bytes(payload)
    monkeypatch.setattr(Path, "glob", _refuse_history_scan)

    with pytest.raises(LegacyQueueStateError, match="sealed index migration state"):
        ClioCoreQueue(root).initialize()

    assert index_path.read_bytes() == payload


@pytest.mark.parametrize("nested", [False, True])
def test_sealed_startup_rejects_duplicate_index_keys_at_every_depth(
    tmp_path: Path,
    nested: bool,
) -> None:
    """A duplicate completion key cannot override sealed migration evidence."""
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    index_path = root / "migrations" / "index-v1.json"
    document = json.loads(index_path.read_bytes())
    payload = json.dumps(document, separators=(",", ":"))
    needle = '"jobs":{"cursor":null,"complete":true}' if nested else '"complete":true'
    replacement = (
        '"jobs":{"cursor":null,"complete":true,"complete":false}'
        if nested
        else '"complete":true,"complete":false'
    )
    assert needle in payload
    payload = payload.replace(needle, replacement, 1).encode("utf-8")
    index_path.write_bytes(payload)

    with pytest.raises(LegacyQueueStateError, match="sealed index migration state"):
        ClioCoreQueue(root).initialize()

    assert index_path.read_bytes() == payload


def test_sealed_startup_rejects_duplicate_legacy_output_marker_key(
    tmp_path: Path,
) -> None:
    """Duplicate completion keys cannot override legacy-output migration evidence."""
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    marker_path = root / "migrations" / "legacy-output-v1.json"
    document = json.loads(marker_path.read_bytes())
    payload = json.dumps(document, separators=(",", ":"))
    assert '"complete":true' in payload
    payload = payload.replace(
        '"complete":true',
        '"complete":true,"complete":false',
        1,
    ).encode("utf-8")
    marker_path.write_bytes(payload)

    with pytest.raises(LegacyQueueStateError, match="legacy-output marker"):
        ClioCoreQueue(root).initialize()

    assert marker_path.read_bytes() == payload


def test_sealed_startup_bounded_transition_scan_rejects_unsafe_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsafe transition layout fails without falling back to an unbounded glob."""
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    (root / "transition_intents" / "unsafe.txt").write_text("unsafe", encoding="utf-8")
    monkeypatch.setattr(Path, "glob", _refuse_history_scan)

    with pytest.raises(QueueConflictError, match="contains an unsafe record"):
        ClioCoreQueue(root).initialize()


def test_missing_seal_is_repaired_only_after_a_complete_bounded_audit(
    tmp_path: Path,
) -> None:
    """A missing seal is recoverable, but never trusted without one full audit."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    queue.submit_job(_job("job_reseal"))
    marker = _audit_marker(root)
    marker.unlink()

    ClioCoreQueue(root).initialize()

    document = json.loads(marker.read_bytes())
    assert document == ClioCoreQueue._legacy_record_audit_marker()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_missing_seal_runs_exactly_one_full_audit_under_the_queue_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seal repair never doubles the history-sized audit or runs it unlocked."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    queue.submit_job(_job("job_single_audit"))
    _audit_marker(root).unlink()
    original_audit = queue_legacy_audit.audit_before_initialization
    lock_observations: list[bool] = []

    def observe_audit(candidate: ClioCoreQueue) -> object:
        lock_observations.append(candidate._lock.is_locked)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        return original_audit(candidate)

    monkeypatch.setattr(
        queue_legacy_audit,
        "audit_before_initialization",
        observe_audit,
    )

    queue.initialize()

    assert lock_observations == [True]


def test_missing_seal_repair_fails_closed_at_the_scan_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seal loss cannot turn bounded startup repair into an unbounded scan."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    for index in range(3):
        queue.submit_job(_job(f"job_repair_bound_{index}"))
    marker = _audit_marker(root)
    marker.unlink()
    monkeypatch.setattr(queue_legacy_audit.queue_layout, "MAX_BOUNDED_SCAN_RECORDS", 2)

    with pytest.raises(LegacyQueueStateError, match="bounded legacy audit limit"):
        ClioCoreQueue(root).initialize()

    assert not marker.exists()


def test_crash_after_durable_seal_recovers_without_reauditing_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A process crash after seal replacement leaves a complete restart boundary."""
    root = tmp_path / "core"

    def crash_after_marker(phase: str, _path: Path) -> None:
        if phase == "marker":
            raise RuntimeError("simulated post-seal crash")

    monkeypatch.setattr(
        queue_legacy_audit,
        "after_audit_phase",
        staticmethod(crash_after_marker),
    )
    with pytest.raises(RuntimeError, match="post-seal crash"):
        ClioCoreQueue(root).initialize()

    marker = _audit_marker(root)
    assert json.loads(marker.read_bytes()) == ClioCoreQueue._legacy_record_audit_marker()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    monkeypatch.setattr(
        queue_legacy_audit,
        "after_audit_phase",
        staticmethod(_no_audit_fault),
    )
    monkeypatch.setattr(
        queue_legacy_audit,
        "audit_before_initialization",
        _refuse_history_scan,
    )
    ClioCoreQueue(root).initialize()


@pytest.mark.parametrize(
    "payload",
    [
        b'{"schema_version":"clio-relay.legacy-record-audit.v1","complete":false}',
        b'{"schema_version":"clio-relay.legacy-record-audit.v1",',
    ],
)
def test_malformed_or_incomplete_seal_fails_closed_without_repair(
    tmp_path: Path,
    payload: bytes,
) -> None:
    """A present but invalid seal is tamper evidence, never an implicit repair request."""
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    marker = _audit_marker(root)
    marker.write_bytes(payload)
    index_before = (root / "migrations" / "index-v1.json").read_bytes()

    with pytest.raises(LegacyQueueStateError, match="legacy-record audit marker"):
        ClioCoreQueue(root).initialize()

    assert marker.read_bytes() == payload
    assert (root / "migrations" / "index-v1.json").read_bytes() == index_before


def test_indexed_seal_refuses_a_missing_family_without_recreating_it(tmp_path: Path) -> None:
    """Fixed-layout validation fails before a deleted family can be silently repaired."""
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    family = root / "monitor_rules"
    shutil.rmtree(family)

    with pytest.raises(LegacyQueueStateError, match="owned record directory"):
        ClioCoreQueue(root).initialize()

    assert not family.exists()


def test_post_seal_canonical_tamper_is_rejected_when_record_is_accessed(
    tmp_path: Path,
) -> None:
    """O(1) startup defers one record's integrity check to its exact read."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    queue.submit_job(_job("job_original"))
    (root / "jobs" / "job_original.json").write_text(
        _job("job_substituted").model_dump_json(),
        encoding="utf-8",
    )

    reopened = ClioCoreQueue(root)
    reopened.initialize()
    with pytest.raises(QueueConflictError, match="canonical job identity mismatch"):
        reopened.get_job("job_original")


def test_incomplete_migration_validates_canonical_identity_before_index_mutation(
    tmp_path: Path,
) -> None:
    """A post-seal identity substitution cannot be projected into derived indexes."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    queue.submit_job(_job("job_migration_source"))
    index_path = root / "migrations" / "index-v1.json"
    state = json.loads(index_path.read_bytes())
    state["complete"] = False
    state["families"]["jobs"] = {"cursor": None, "complete": False}
    index_path.write_text(json.dumps(state), encoding="utf-8")
    substituted = _job("job_migration_substituted")
    (root / "jobs" / "job_migration_source.json").write_text(
        substituted.model_dump_json(),
        encoding="utf-8",
    )

    with pytest.raises(QueueConflictError, match="canonical job identity mismatch"):
        ClioCoreQueue(root).migrate_indexes_batch(batch_size=10)

    assert not (root / "jobs" / "job_migration_substituted.json").exists()
    assert not (root / "job_indexes" / "job_migration_substituted.json").exists()

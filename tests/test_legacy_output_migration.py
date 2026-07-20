"""Production upgrade coverage for v0.9 duplicated output events."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Literal

import pytest

import clio_relay.core_queue as core_queue_module
from clio_relay.core_queue import ClioCoreQueue, LegacyQueueStateError
from clio_relay.errors import QueueConflictError
from clio_relay.models import JobKind, JobState, JobTombstone, RelayEvent, utc_now


def _event_path(root: Path, job_id: str, seq: int) -> Path:
    path = root / "events" / job_id / f"{seq:020d}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _write_event(root: Path, event: RelayEvent) -> bytes:
    payload = event.model_dump_json(indent=2).encode("utf-8")
    _event_path(root, event.job_id, event.seq).write_bytes(payload)
    return payload


def _write_v09_output_event(
    root: Path,
    *,
    job_id: str = "job_legacy_output",
    seq: int = 1,
    text: str,
    stream: Literal["stdout", "stderr"] = "stdout",
) -> bytes:
    event = RelayEvent(
        job_id=job_id,
        seq=seq,
        event_type=f"{stream}.delta",
        message=text.rstrip("\n") or f"{stream} output",
        payload={"stream": stream, "text": text},
    )
    return _write_event(root, event)


def _archive_path(root: Path, job_id: str, seq: int) -> Path:
    return root / "legacy_output_archives" / job_id / f"{seq:020d}.json"


def _receipt_path(root: Path, job_id: str, seq: int) -> Path:
    return root / "legacy_output_receipts" / job_id / f"{seq:020d}.json"


def _marker_path(root: Path) -> Path:
    return root / "migrations" / "legacy-output-v1.json"


def _record_audit_marker_path(root: Path) -> Path:
    return root / "migrations" / "legacy-record-audit-v1.json"


def _no_migration_fault(_phase: str, _path: Path) -> None:
    return


def test_legacy_output_migration_requires_explicit_authorization(tmp_path: Path) -> None:
    """Ordinary startup cannot mutate valid v0.9 output without an explicit operator gate."""
    root = tmp_path / "core"
    original = _write_v09_output_event(root, text=("authorization\n" * 40_000))

    with pytest.raises(LegacyQueueStateError) as raised:
        ClioCoreQueue(root).initialize()

    assert "clio-relay init --migrate-legacy-output" in raised.value.report["action"]
    assert _event_path(root, "job_legacy_output", 1).read_bytes() == original
    assert not (root / "legacy_output_archives").exists()
    assert not (root / "legacy_output_receipts").exists()
    assert not _marker_path(root).exists()

    ClioCoreQueue(root).initialize(migrate_legacy_output=True)
    assert _marker_path(root).is_file()


def test_multimegabyte_v09_output_is_archived_and_sequence_is_preserved(
    tmp_path: Path,
) -> None:
    """A multi-MiB callback becomes one bounded same-sequence archive reference."""
    root = tmp_path / "core"
    job_id = "job_multimegabyte"
    first = RelayEvent(job_id=job_id, seq=1, event_type="job.started", message="started")
    last = RelayEvent(job_id=job_id, seq=3, event_type="job.succeeded", message="done")
    first_bytes = _write_event(root, first)
    original = _write_v09_output_event(
        root,
        job_id=job_id,
        seq=2,
        text=("large-output-line\n" * 140_000),
    )
    last_bytes = _write_event(root, last)
    assert len(original) > 2 * 1_048_576

    ClioCoreQueue(root).initialize(migrate_legacy_output=True)

    event_directory = root / "events" / job_id
    assert [path.name for path in sorted(event_directory.glob("*.json"))] == [
        "00000000000000000001.json",
        "00000000000000000002.json",
        "00000000000000000003.json",
    ]
    assert (event_directory / "00000000000000000001.json").read_bytes() == first_bytes
    assert (event_directory / "00000000000000000003.json").read_bytes() == last_bytes
    replacement_path = event_directory / "00000000000000000002.json"
    replacement_bytes = replacement_path.read_bytes()
    replacement = RelayEvent.model_validate_json(replacement_bytes)
    assert replacement.seq == 2
    assert replacement.event_type == "stdout.delta"
    assert len(replacement_bytes) <= core_queue_module.RECORD_FAMILY_MAX_BYTES["events"]
    compatibility = replacement.payload["legacy_output"]
    assert compatibility["representation"] == "archive"
    archive = _archive_path(root, job_id, 2)
    assert archive.read_bytes() == original
    receipt = json.loads(_receipt_path(root, job_id, 2).read_bytes())
    assert receipt["archive_sha256"] == hashlib.sha256(original).hexdigest()
    assert receipt["archive_size_bytes"] == len(original)
    assert receipt["replacement_sha256"] == hashlib.sha256(replacement_bytes).hexdigest()
    assert receipt["replacement_size_bytes"] == len(replacement_bytes)
    marker = json.loads(_marker_path(root).read_bytes())
    assert marker["complete"] is True
    assert marker["event_records"] == 3
    assert marker["migration_records"] == 1
    assert marker["archive_bytes"] == len(original)


def test_migration_preserves_payload_text_that_fits_the_normal_event_cap(
    tmp_path: Path,
) -> None:
    """A moderately oversized duplicate preserves the historical payload contract."""
    root = tmp_path / "core"
    text = ("visible-output" * 12_000) + "\n"
    original = _write_v09_output_event(root, text=text)
    assert len(original) > core_queue_module.RECORD_FAMILY_MAX_BYTES["events"]

    ClioCoreQueue(root).initialize(migrate_legacy_output=True)

    replacement = RelayEvent.model_validate_json(
        _event_path(root, "job_legacy_output", 1).read_bytes()
    )
    assert replacement.message.startswith("Legacy stdout output preserved in payload.text")
    assert replacement.payload["legacy_output"]["representation"] == "payload_text"
    assert replacement.payload["text"] == text
    assert _archive_path(root, "job_legacy_output", 1).read_bytes() == original


def test_newline_only_v09_output_uses_the_exact_fallback_message(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The v0.9 fallback message remains eligible when output is only newlines."""
    root = tmp_path / "core"
    monkeypatch.setitem(core_queue_module.RECORD_FAMILY_MAX_BYTES, "events", 2_048)
    original = _write_v09_output_event(root, text="\n" * 2_000)
    assert len(original) > core_queue_module.RECORD_FAMILY_MAX_BYTES["events"]

    ClioCoreQueue(root).initialize(migrate_legacy_output=True)

    replacement = RelayEvent.model_validate_json(
        _event_path(root, "job_legacy_output", 1).read_bytes()
    )
    assert replacement.message.startswith("Legacy stdout output archived")
    assert replacement.payload["legacy_output"]["representation"] == "archive"
    assert _archive_path(root, "job_legacy_output", 1).read_bytes() == original


@pytest.mark.parametrize("phase", ["archive", "replacement", "receipt", "marker"])
def test_migration_recovers_idempotently_from_each_durable_phase_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    """Every durable migration phase is restartable without rewriting evidence."""
    root = tmp_path / phase
    job_id = f"job_crash_{phase}"
    original = _write_v09_output_event(
        root,
        job_id=job_id,
        text=("crash-recovery\n" * 120_000),
    )
    failed = False

    def crash_after(selected_phase: str, _path: Path) -> None:
        nonlocal failed
        if selected_phase == phase and not failed:
            failed = True
            raise RuntimeError(f"simulated {phase} crash")

    monkeypatch.setattr(
        ClioCoreQueue,
        "_after_legacy_output_migration_phase",
        staticmethod(crash_after),
    )
    with pytest.raises(RuntimeError, match=f"simulated {phase} crash"):
        ClioCoreQueue(root).initialize(migrate_legacy_output=True)

    archive = _archive_path(root, job_id, 1)
    event = _event_path(root, job_id, 1)
    assert archive.read_bytes() == original
    assert _receipt_path(root, job_id, 1).exists() is (phase in {"receipt", "marker"})
    assert _marker_path(root).exists() is (phase == "marker")
    if phase == "archive":
        assert event.read_bytes() == original
    else:
        assert event.read_bytes() != original

    monkeypatch.setattr(
        ClioCoreQueue,
        "_after_legacy_output_migration_phase",
        staticmethod(_no_migration_fault),
    )
    ClioCoreQueue(root).initialize(migrate_legacy_output=True)
    completed_event = event.read_bytes()
    completed_archive = archive.read_bytes()
    completed_receipt = _receipt_path(root, job_id, 1).read_bytes()
    completed_marker = _marker_path(root).read_bytes()

    ClioCoreQueue(root).initialize()

    assert event.read_bytes() == completed_event
    assert archive.read_bytes() == completed_archive == original
    assert _receipt_path(root, job_id, 1).read_bytes() == completed_receipt
    assert _marker_path(root).read_bytes() == completed_marker


def test_complete_marker_skips_later_deep_event_history_scans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The durable completion marker makes subsequent startup constant in history size."""
    root = tmp_path / "core"
    _write_v09_output_event(root, text=("marker\n" * 80_000))
    ClioCoreQueue(root).initialize(migrate_legacy_output=True)
    original_iterator = ClioCoreQueue._iter_legacy_event_paths  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def refuse_history_scan(
        _self: ClioCoreQueue,
        _family: str,
        *,
        max_directories: int,
        max_records: int,
    ) -> object:
        if _family == "events":
            raise AssertionError("completed migration must not scan event history")
        return original_iterator(
            _self,
            _family,
            max_directories=max_directories,
            max_records=max_records,
        )

    monkeypatch.setattr(ClioCoreQueue, "_iter_legacy_event_paths", refuse_history_scan)
    ClioCoreQueue(root).initialize()


@pytest.mark.parametrize(
    "family",
    ["legacy_output_archives", "legacy_output_receipts", "legacy_output_retired"],
)
def test_complete_marker_requires_every_owned_evidence_root(
    tmp_path: Path,
    family: str,
) -> None:
    """A completion marker cannot silently recreate a deleted evidence family."""
    root = tmp_path / family
    _write_v09_output_event(root, text=("owned-root\n" * 40_000))
    ClioCoreQueue(root).initialize(migrate_legacy_output=True)
    shutil.rmtree(root / family)

    with pytest.raises(LegacyQueueStateError, match="requires its owned record directory"):
        ClioCoreQueue(root).initialize()

    assert not (root / family).exists()


@pytest.mark.parametrize("deleted", ["archive", "receipt", "all"])
def test_complete_marker_detects_deleted_migration_evidence(
    tmp_path: Path,
    deleted: str,
) -> None:
    """Indexed startup is O(1); access and bounded reseal still detect evidence loss."""
    root = tmp_path / deleted
    job_id = "job_marker_evidence"
    _write_v09_output_event(
        root,
        job_id=job_id,
        text=("marker-evidence\n" * 40_000),
    )
    ClioCoreQueue(root).initialize(migrate_legacy_output=True)
    if deleted in {"archive", "all"}:
        _archive_path(root, job_id, 1).unlink()
    if deleted in {"receipt", "all"}:
        _receipt_path(root, job_id, 1).unlink()
    if deleted == "all":
        _event_path(root, job_id, 1).unlink()

    reopened = ClioCoreQueue(root)
    reopened.initialize()
    if deleted != "all":
        with pytest.raises(LegacyQueueStateError):
            reopened.read_event_page(job_id)

    _record_audit_marker_path(root).unlink()
    with pytest.raises(LegacyQueueStateError):
        ClioCoreQueue(root).initialize()


def test_complete_marker_detects_schema_valid_retired_receipt_corruption(
    tmp_path: Path,
) -> None:
    """A bounded explicit reseal authenticates retired evidence without startup scans."""
    root = tmp_path / "retired-corruption"
    job_id = "job_retired_corruption"
    _write_v09_output_event(
        root,
        job_id=job_id,
        text=("retired-corruption\n" * 40_000),
    )
    queue = ClioCoreQueue(root)
    queue.initialize(migrate_legacy_output=True)
    now = utc_now()
    tombstone = JobTombstone(
        job_id=job_id,
        cluster="ares",
        kind=JobKind.REMOTE_AGENT,
        final_state=JobState.SUCCEEDED,
        idempotency_key="retired-corruption",
        job_digest="b" * 64,
        created_at=now,
        updated_at=now,
        external_quarantine_id="retired-corruption",
        records_trash_started=True,
    )
    queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._job_tombstone_path(job_id),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tombstone,
    )
    queue._trash_job_roots_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tombstone,
        limit=100,
    )
    retired = root / "legacy_output_retired" / job_id / "00000000000000000001.json"
    receipt = json.loads(retired.read_bytes())
    receipt["archive_sha256"] = "c" * 64
    retired.write_text(json.dumps(receipt), encoding="utf-8")

    ClioCoreQueue(root).initialize()
    _record_audit_marker_path(root).unlink()
    with pytest.raises(LegacyQueueStateError, match="marker totals"):
        ClioCoreQueue(root).initialize()


@pytest.mark.parametrize("limit", [1, 2, 3])
def test_retired_receipt_manifest_survives_each_gc_root_move(
    tmp_path: Path,
    limit: int,
) -> None:
    """Startup accepts only tombstone-authorized receipt retirement across GC crashes."""
    root = tmp_path / f"gc-{limit}"
    job_id = f"job_gc_phase_{limit}"
    _write_v09_output_event(
        root,
        job_id=job_id,
        text=("gc-phase\n" * 40_000),
    )
    queue = ClioCoreQueue(root)
    queue.initialize(migrate_legacy_output=True)
    now = utc_now()
    tombstone = JobTombstone(
        job_id=job_id,
        cluster="ares",
        kind=JobKind.REMOTE_AGENT,
        final_state=JobState.SUCCEEDED,
        idempotency_key=f"gc-phase-{limit}",
        job_digest="d" * 64,
        created_at=now,
        updated_at=now,
        external_quarantine_id=f"gc-phase-{limit}",
        records_trash_started=True,
    )
    queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._job_tombstone_path(job_id),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tombstone,
    )

    moved, _complete = queue._trash_job_roots_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tombstone,
        limit=limit,
    )

    assert moved == limit
    assert not (root / "legacy_output_receipts" / job_id).exists()
    assert (root / "legacy_output_retired" / job_id / "00000000000000000001.json").is_file()
    ClioCoreQueue(root).initialize()


def test_event_audit_streams_beyond_the_previous_global_scan_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Event history no longer inherits the old 10k all-family materialization cap."""
    root = tmp_path / "core"
    job_id = "job_many_events"
    for seq in range(1, 6):
        _write_event(
            root,
            RelayEvent(
                job_id=job_id,
                seq=seq,
                event_type="stdout.delta",
                message=f"line {seq}",
                payload={"stream": "stdout", "text": f"line {seq}\n"},
            ),
        )
    monkeypatch.setattr(core_queue_module, "MAX_BOUNDED_SCAN_RECORDS", 2)
    monkeypatch.setattr(core_queue_module, "MAX_LEGACY_EVENT_AUDIT_RECORDS", 10)

    ClioCoreQueue(root).initialize()

    marker = json.loads(_marker_path(root).read_bytes())
    assert marker["event_records"] == 5
    assert marker["migration_records"] == 0


def test_all_legacy_families_are_validated_before_any_output_archive_write(
    tmp_path: Path,
) -> None:
    """An unrelated unsafe canonical record prevents every migration mutation."""
    root = tmp_path / "core"
    _write_v09_output_event(root, text=("blocked\n" * 80_000))
    jobs = root / "jobs"
    jobs.mkdir()
    (jobs / "Unsafe Job.json").write_text("{}", encoding="utf-8")

    with pytest.raises(LegacyQueueStateError):
        ClioCoreQueue(root).initialize()

    assert not (root / "legacy_output_archives").exists()
    assert not (root / "legacy_output_receipts").exists()
    assert not (root / "migrations").exists()


def test_unknown_oversized_output_shape_fails_closed(tmp_path: Path) -> None:
    """Only the exact v0.9 duplicated delta shape is eligible for migration."""
    root = tmp_path / "core"
    text = "x" * 180_000
    event = RelayEvent(
        job_id="job_unknown_shape",
        seq=1,
        event_type="stdout.delta",
        message=text,
        payload={"stream": "stdout", "text": f"different-{text}"},
    )
    original = _write_event(root, event)
    assert len(original) > core_queue_module.RECORD_FAMILY_MAX_BYTES["events"]

    with pytest.raises(LegacyQueueStateError, match="exact duplicated v0.9 delta"):
        ClioCoreQueue(root).initialize()

    assert _event_path(root, event.job_id, event.seq).read_bytes() == original
    assert not (root / "legacy_output_archives").exists()


def test_legacy_output_per_record_and_aggregate_budgets_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both the individual read budget and aggregate archive budget are enforced."""
    per_record_root = tmp_path / "per-record"
    per_record = _write_v09_output_event(
        per_record_root,
        text=("per-record\n" * 40_000),
    )
    monkeypatch.setattr(
        core_queue_module,
        "MAX_LEGACY_OUTPUT_RECORD_BYTES",
        len(per_record) - 1,
    )
    with pytest.raises(LegacyQueueStateError, match="bounded compatibility limit"):
        ClioCoreQueue(per_record_root).initialize()
    assert not (per_record_root / "legacy_output_archives").exists()

    monkeypatch.setattr(
        core_queue_module,
        "MAX_LEGACY_OUTPUT_RECORD_BYTES",
        16 * 1_048_576,
    )
    aggregate_root = tmp_path / "aggregate"
    first = _write_v09_output_event(
        aggregate_root,
        job_id="job_aggregate",
        seq=1,
        text=("aggregate-one\n" * 30_000),
    )
    second = _write_v09_output_event(
        aggregate_root,
        job_id="job_aggregate",
        seq=2,
        text=("aggregate-two\n" * 30_000),
    )
    monkeypatch.setattr(
        core_queue_module,
        "MAX_LEGACY_OUTPUT_MIGRATION_BYTES",
        len(first) + len(second) - 1,
    )
    with pytest.raises(LegacyQueueStateError, match="aggregate byte limit"):
        ClioCoreQueue(aggregate_root).initialize()
    assert not (aggregate_root / "legacy_output_archives").exists()


def test_hard_linked_legacy_event_and_archive_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Neither a source event nor a recovery archive may have another hard link."""
    source_root = tmp_path / "source-link"
    source = _event_path(source_root, "job_linked_source", 1)
    source.write_bytes(
        _write_v09_output_event(
            tmp_path / "source-template",
            job_id="job_linked_source",
            text=("linked-source\n" * 40_000),
        )
    )
    os.link(source, source_root / "operator-hard-link.json")
    with pytest.raises(LegacyQueueStateError):
        ClioCoreQueue(source_root).initialize()
    assert not (source_root / "legacy_output_archives").exists()

    archive_root = tmp_path / "archive-link"
    _write_v09_output_event(
        archive_root,
        job_id="job_linked_archive",
        text=("linked-archive\n" * 40_000),
    )

    def crash_after_archive(phase: str, _path: Path) -> None:
        if phase == "archive":
            raise RuntimeError("archive ready")

    monkeypatch.setattr(
        ClioCoreQueue,
        "_after_legacy_output_migration_phase",
        staticmethod(crash_after_archive),
    )
    with pytest.raises(RuntimeError, match="archive ready"):
        ClioCoreQueue(archive_root).initialize(migrate_legacy_output=True)
    archive = _archive_path(archive_root, "job_linked_archive", 1)
    os.link(archive, archive_root / "operator-archive-hard-link.json")
    monkeypatch.setattr(
        ClioCoreQueue,
        "_after_legacy_output_migration_phase",
        staticmethod(_no_migration_fault),
    )
    with pytest.raises(LegacyQueueStateError):
        ClioCoreQueue(archive_root).initialize(migrate_legacy_output=True)
    assert not _marker_path(archive_root).exists()


def test_current_event_writes_remain_bounded_to_256_kib(tmp_path: Path) -> None:
    """The compatibility archive limit never weakens ordinary event writes."""
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    text = "x" * 180_000

    with pytest.raises(QueueConflictError, match="262144-byte limit"):
        queue.append_event(
            "job_current_write",
            "stdout.delta",
            text,
            payload={"stream": "stdout", "text": f"{text}\n"},
        )

    assert not list((queue.root / "events" / "job_current_write").glob("*.json"))


def test_terminal_job_gc_retires_receipts_before_trashing_large_output_roots(
    tmp_path: Path,
) -> None:
    """Small receipts remain durable while event and archive roots enter quarantine."""
    root = tmp_path / "core"
    job_id = "job_legacy_gc"
    _write_v09_output_event(
        root,
        job_id=job_id,
        text=("gc-owned-output\n" * 40_000),
    )
    queue = ClioCoreQueue(root)
    queue.initialize(migrate_legacy_output=True)
    now = utc_now()
    tombstone = JobTombstone(
        job_id=job_id,
        cluster="ares",
        kind=JobKind.REMOTE_AGENT,
        final_state=JobState.SUCCEEDED,
        idempotency_key="legacy-gc",
        job_digest="a" * 64,
        created_at=now,
        updated_at=now,
        external_quarantine_id="test-quarantine",
        records_trash_started=True,
    )
    queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._job_tombstone_path(job_id),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tombstone,
    )

    moved, complete = queue._trash_job_roots_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tombstone,
        limit=100,
    )

    trash = root / "gc_trash" / job_id / "owned"
    assert complete is True
    assert moved >= 3
    assert not (root / "events" / job_id).exists()
    assert not (root / "legacy_output_archives" / job_id).exists()
    assert not (root / "legacy_output_receipts" / job_id).exists()
    assert (root / "legacy_output_retired" / job_id / "00000000000000000001.json").is_file()
    assert (trash / "events" / "00000000000000000001.json").is_file()
    assert (trash / "legacy_output_archives" / "00000000000000000001.json").is_file()

    ClioCoreQueue(root).initialize()

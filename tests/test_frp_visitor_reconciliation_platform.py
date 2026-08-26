"""Cross-platform snapshot/lookup parsing and D2 typed-skip-reason coverage
for :mod:`clio_relay.frp_visitor_process_inspection`, plus the
``reconcile_stale_frp_visitors`` orchestrator (incl. D8's once-per-process
sweep gate) in :mod:`clio_relay.frp_visitor_reconciliation`.

Split out of ``test_frp_visitor_reconciliation.py`` (classification, D1
pid-reuse re-verify, D3 reap/sweep secret removal) to stay under the
file-size sweet spot -- see that file's own docstring for the shared
"fake records only, never a real OS process" discipline both files follow.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

from clio_relay import frp_visitor_process_inspection as inspection
from clio_relay import frp_visitor_reconciliation as reconciliation
from clio_relay.frp_visitor_process_inspection import (
    SNAPSHOT_SKIP_EXIT_STATUS,
    SNAPSHOT_SKIP_MALFORMED_OUTPUT,
    SNAPSHOT_SKIP_OSERROR,
    SNAPSHOT_SKIP_TIMEOUT,
    ProcessRecord,
    ProcessSnapshot,
)
from clio_relay.frp_visitor_reconciliation import (
    ReapOutcome,
    ReconciliationResult,
    reap_stale_frp_visitors,
    reconcile_stale_frp_visitors,
)

FRPC_BIN = "frpc"


@pytest.fixture(autouse=True)
def _reset_sweep_once_gate(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]
    """D8's once-per-process sweep gate must not leak state across tests."""
    monkeypatch.setattr(reconciliation, "_sweep_has_run", False)


def _visitor_cmdline(
    *, frpc_bin: str = FRPC_BIN, config_dir: str = "clio-relay-frp-visitor-abc123"
) -> str:
    return f"{frpc_bin} -c /tmp/{config_dir}/frpc-visitor.toml"


def _snapshot(records: list[ProcessRecord]) -> ProcessSnapshot:
    return ProcessSnapshot(records=tuple(records))


def _lookup_returning(record: ProcessRecord | None) -> reconciliation.SingleProcessLookup:
    return lambda _pid: record


def _backdate(path: Path, *, seconds_ago: float) -> None:
    stamp = time.time() - seconds_ago
    os.utime(path, (stamp, stamp))


# default_process_snapshot dispatch + platform parsers (frp_visitor_process_inspection.py)


def test_default_process_snapshot_dispatches_to_windows_on_nt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(inspection.os, "name", "nt")
    sentinel = _snapshot([ProcessRecord(pid=1, parent_pid=None, cmdline="x")])
    monkeypatch.setattr(inspection, "_windows_process_snapshot", lambda: sentinel)

    assert inspection.default_process_snapshot() == sentinel


def test_default_process_snapshot_dispatches_to_posix_off_nt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(inspection.os, "name", "posix")
    sentinel = _snapshot([ProcessRecord(pid=2, parent_pid=None, cmdline="y")])
    monkeypatch.setattr(inspection, "_posix_process_snapshot", lambda: sentinel)

    assert inspection.default_process_snapshot() == sentinel


def _write_fake_proc_entry(
    proc_root: Path,
    *,
    pid: int,
    parent_pid: int,
    cmdline_argv: list[str],
) -> None:
    entry = proc_root / str(pid)
    entry.mkdir(parents=True)
    (entry / "stat").write_text(f"{pid} (frpc) S {parent_pid} {pid} 0\n", encoding="utf-8")
    (entry / "cmdline").write_bytes(("\x00".join(cmdline_argv) + "\x00").encode("utf-8"))


def _posix_process_snapshot(proc_root: Path) -> ProcessSnapshot:
    return inspection._posix_process_snapshot(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        proc_root
    )


def test_posix_process_snapshot_parses_pid_parent_and_cmdline(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    _write_fake_proc_entry(
        proc_root,
        pid=111,
        parent_pid=222,
        cmdline_argv=["frpc", "-c", "/tmp/clio-relay-frp-visitor-x/frpc-visitor.toml"],
    )
    (proc_root / "not-a-pid").mkdir(parents=True)  # must be ignored, not a digit name

    snapshot = _posix_process_snapshot(proc_root)

    assert snapshot.skipped_reason is None
    assert len(snapshot.records) == 1
    record = snapshot.records[0]
    assert record.pid == 111
    assert record.parent_pid == 222
    assert "clio-relay-frp-visitor-x" in record.cmdline


def test_posix_process_snapshot_skips_an_entry_that_vanishes_mid_read(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir(parents=True)
    (proc_root / "999").mkdir()  # no stat/cmdline files inside -- simulates a race

    snapshot = _posix_process_snapshot(proc_root)

    assert snapshot == ProcessSnapshot(records=())


def test_posix_process_snapshot_reports_a_typed_skip_reason_for_a_missing_root(
    tmp_path: Path,
) -> None:
    """D2: an unreadable /proc itself is a distinct failure, not silent 'nothing found'."""
    snapshot = _posix_process_snapshot(tmp_path / "does-not-exist")

    assert snapshot.records == ()
    assert snapshot.skipped_reason == SNAPSHOT_SKIP_OSERROR


class _FakeCompletedProcess:
    def __init__(self, *, returncode: int, stdout: str) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = ""


def _windows_process_snapshot() -> ProcessSnapshot:
    return inspection._windows_process_snapshot()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_windows_process_snapshot_parses_powershell_json(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = (
        '[{"ProcessId": 111, "ParentProcessId": 222, '
        '"CommandLine": "frpc -c C:/temp/clio-relay-frp-visitor-x/frpc-visitor.toml"},'
        '{"ProcessId": 0, "ParentProcessId": 0, "CommandLine": null}]'
    )

    def fake_run(*_args: object, **_kwargs: object) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(returncode=0, stdout=payload)

    monkeypatch.setattr(inspection.subprocess, "run", fake_run)

    snapshot = _windows_process_snapshot()

    assert snapshot.skipped_reason is None
    assert len(snapshot.records) == 1  # the PID-0 sentinel row is discarded
    assert snapshot.records[0].pid == 111
    assert snapshot.records[0].parent_pid == 222
    assert "clio-relay-frp-visitor-x" in snapshot.records[0].cmdline


# D2: every one of the four windows-snapshot failure modes gets its OWN typed reason.


def test_windows_process_snapshot_skip_reason_on_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(returncode=1, stdout="")

    monkeypatch.setattr(inspection.subprocess, "run", fake_run)

    snapshot = _windows_process_snapshot()

    assert snapshot.records == ()
    assert snapshot.skipped_reason == SNAPSHOT_SKIP_EXIT_STATUS


def test_windows_process_snapshot_skip_reason_on_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(returncode=0, stdout="not json")

    monkeypatch.setattr(inspection.subprocess, "run", fake_run)

    snapshot = _windows_process_snapshot()

    assert snapshot.records == ()
    assert snapshot.skipped_reason == SNAPSHOT_SKIP_MALFORMED_OUTPUT


def test_windows_process_snapshot_skip_reason_on_empty_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(returncode=0, stdout="   ")

    monkeypatch.setattr(inspection.subprocess, "run", fake_run)

    snapshot = _windows_process_snapshot()

    assert snapshot.records == ()
    assert snapshot.skipped_reason == SNAPSHOT_SKIP_MALFORMED_OUTPUT


def test_windows_process_snapshot_skip_reason_when_powershell_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(*_args: object, **_kwargs: object) -> None:
        raise OSError("powershell not found")

    monkeypatch.setattr(inspection.subprocess, "run", _raise)

    snapshot = _windows_process_snapshot()

    assert snapshot.records == ()
    assert snapshot.skipped_reason == SNAPSHOT_SKIP_OSERROR


def test_windows_process_snapshot_skip_reason_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_args: object, **_kwargs: object) -> None:
        raise subprocess.TimeoutExpired(cmd="powershell", timeout=20)

    monkeypatch.setattr(inspection.subprocess, "run", _raise)

    snapshot = _windows_process_snapshot()

    assert snapshot.records == ()
    assert snapshot.skipped_reason == SNAPSHOT_SKIP_TIMEOUT


def test_enumeration_timeout_is_bounded_sane() -> None:
    """D8: was 20s (a functional hang budget on every visitor spawn); now 5s."""
    assert inspection.DEFAULT_ENUMERATION_TIMEOUT_SECONDS == 5.0


# reap_stale_frp_visitors propagating a skipped snapshot (D2)


def test_reap_reports_a_skipped_snapshot_rather_than_reaping_nothing_silently() -> None:
    skipped = ProcessSnapshot(records=(), skipped_reason=SNAPSHOT_SKIP_TIMEOUT)

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: skipped,
        terminate_process=lambda _pid: pytest.fail("must never be called"),
    )

    assert outcome == ReapOutcome(reaped_pids=(), skipped_reason=SNAPSHOT_SKIP_TIMEOUT)


# default_single_process_lookup dispatch + platform single-pid readers (D1)


def test_default_single_process_lookup_dispatches_to_windows_on_nt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(inspection.os, "name", "nt")
    sentinel = ProcessRecord(pid=7, parent_pid=None, cmdline="x")

    def fake_windows_lookup(_pid: int) -> ProcessRecord | None:
        return sentinel

    monkeypatch.setattr(inspection, "_windows_single_process_record", fake_windows_lookup)

    assert inspection.default_single_process_lookup(7) is sentinel


def test_default_single_process_lookup_dispatches_to_posix_off_nt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(inspection.os, "name", "posix")
    sentinel = ProcessRecord(pid=8, parent_pid=None, cmdline="y")

    def fake_posix_lookup(_pid: int) -> ProcessRecord | None:
        return sentinel

    monkeypatch.setattr(inspection, "_posix_single_process_record", fake_posix_lookup)

    assert inspection.default_single_process_lookup(8) is sentinel


def test_posix_single_process_record_reads_a_fresh_entry(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    _write_fake_proc_entry(proc_root, pid=321, parent_pid=1, cmdline_argv=["frpc", "-c", "/x"])

    record = inspection._posix_single_process_record(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        321, proc_root
    )

    assert record is not None
    assert record.pid == 321
    assert record.parent_pid == 1


def test_posix_single_process_record_returns_none_for_a_missing_pid(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()

    record = inspection._posix_single_process_record(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        99999, proc_root
    )

    assert record is None


def test_windows_single_process_record_reads_a_fresh_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = (
        '{"ParentProcessId": 1, '
        '"CommandLine": "frpc -c C:/x/clio-relay-frp-visitor-1/frpc-visitor.toml"}'
    )

    def fake_run(*_args: object, **_kwargs: object) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(returncode=0, stdout=payload)

    monkeypatch.setattr(inspection.subprocess, "run", fake_run)

    record = inspection._windows_single_process_record(55)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert record is not None
    assert record.pid == 55
    assert record.parent_pid == 1
    assert "clio-relay-frp-visitor-1" in record.cmdline


def test_windows_single_process_record_returns_none_when_the_pid_is_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(returncode=3, stdout="")

    monkeypatch.setattr(inspection.subprocess, "run", fake_run)

    record = inspection._windows_single_process_record(56)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert record is None


def test_windows_single_process_record_returns_none_on_a_bad_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(*_args: object, **_kwargs: object) -> None:
        raise OSError("powershell not found")

    monkeypatch.setattr(inspection.subprocess, "run", _raise)

    record = inspection._windows_single_process_record(57)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert record is None


# reconcile_stale_frp_visitors (the orchestrator frp_transport.py calls)


def test_reconcile_composes_reap_and_sweep(tmp_path: Path) -> None:
    stale = tmp_path / "clio-relay-frp-visitor-stale"
    stale.mkdir()
    _backdate(stale, seconds_ago=7200)
    orphan = ProcessRecord(pid=321, parent_pid=None, cmdline=_visitor_cmdline())

    result = reconcile_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([orphan]),
        terminate_process=lambda _pid: None,
        process_lookup=_lookup_returning(orphan),
        temp_root=tmp_path,
        max_config_dir_age_seconds=3600,
    )

    assert result == ReconciliationResult(reaped_pids=(321,), swept_config_dirs=1)


def test_reconcile_propagates_a_skipped_snapshot_reason(tmp_path: Path) -> None:
    skipped = ProcessSnapshot(records=(), skipped_reason=SNAPSHOT_SKIP_OSERROR)

    result = reconcile_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: skipped,
        temp_root=tmp_path,
    )

    assert result.reaped_pids == ()
    assert result.skipped_reason == SNAPSHOT_SKIP_OSERROR


def test_reconcile_never_raises_when_reaping_fails_unexpectedly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Best-effort cleanup must never block a new visitor's own spawn (#285)."""

    def _raise(**_kwargs: object) -> ReapOutcome:
        raise RuntimeError("simulated: process-table inspection blew up")

    monkeypatch.setattr(reconciliation, "reap_stale_frp_visitors", _raise)

    result = reconcile_stale_frp_visitors(frpc_bin=FRPC_BIN)

    assert result == ReconciliationResult(
        reaped_pids=(), swept_config_dirs=0, skipped_reason="reconciliation_failed:exception"
    )


def test_reconcile_never_raises_when_sweeping_fails_unexpectedly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(**_kwargs: object) -> int:
        raise RuntimeError("simulated: sweep blew up")

    monkeypatch.setattr(reconciliation, "_sweep_once", _raise)

    result = reconcile_stale_frp_visitors(frpc_bin=FRPC_BIN)

    assert result == ReconciliationResult(
        reaped_pids=(), swept_config_dirs=0, skipped_reason="reconciliation_failed:exception"
    )


# D8: the sweep runs at most once per process.


def test_sweep_runs_only_once_per_process_across_reconcile_calls(tmp_path: Path) -> None:
    for index in range(2):
        stale = tmp_path / f"clio-relay-frp-visitor-{index}"
        stale.mkdir()
        _backdate(stale, seconds_ago=7200)

    first = reconcile_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([]),
        temp_root=tmp_path,
        max_config_dir_age_seconds=3600,
    )
    second = reconcile_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([]),
        temp_root=tmp_path,
        max_config_dir_age_seconds=3600,
    )

    assert first.swept_config_dirs == 2
    assert second.swept_config_dirs == 0  # the gate, not a second real walk
    # Proves the gate actually skipped the walk, not that there was simply
    # nothing left to sweep the second time.
    remaining = tmp_path / "clio-relay-frp-visitor-still-here"
    remaining.mkdir()
    _backdate(remaining, seconds_ago=7200)
    third = reconcile_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([]),
        temp_root=tmp_path,
        max_config_dir_age_seconds=3600,
    )
    assert third.swept_config_dirs == 0
    assert remaining.exists()


def test_sweep_once_directly_runs_exactly_once(tmp_path: Path) -> None:
    stale = tmp_path / "clio-relay-frp-visitor-solo"
    stale.mkdir()
    _backdate(stale, seconds_ago=7200)

    first = reconciliation._sweep_once(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        temp_root=tmp_path, max_age_seconds=3600
    )
    second = reconciliation._sweep_once(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        temp_root=tmp_path, max_age_seconds=3600
    )

    assert first == 1
    assert second == 0

"""Pure unit coverage for the stale-frpc-visitor reconciliation pass (#285).

Every test here uses FAKE :class:`~clio_relay.frp_visitor_reconciliation.ProcessRecord`
rows (or a real, but throwaway, ``tmp_path``-rooted fake ``/proc`` tree for the
POSIX parser) -- never a real OS process, never a real ``powershell``/``ps``
subprocess. The injectable process-inspection seam
(``reap_stale_frp_visitors``'s ``process_snapshot``/``terminate_process``
parameters) is exactly what makes that possible; the integration-level tests
proving this module's wiring into an actual visitor spawn (typed
``visitor_orphan_reaped`` channel events, the exit-path close) live in
``tests/test_frp_transport_dials.py`` instead, over its own injected-frpc-
process harness.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

from clio_relay import frp_visitor_reconciliation as reconciliation
from clio_relay.frp_visitor_reconciliation import (
    ProcessRecord,
    ReconciliationResult,
    reap_stale_frp_visitors,
    reconcile_stale_frp_visitors,
    sweep_stale_visitor_config_dirs,
)

FRPC_BIN = "frpc"


def _visitor_cmdline(
    *, frpc_bin: str = FRPC_BIN, config_dir: str = "clio-relay-frp-visitor-abc123"
) -> str:
    return f"{frpc_bin} -c /tmp/{config_dir}/frpc-visitor.toml"


# _is_stale_visitor_candidate / _command_names_binary
#
# These tests reach the module's private classification helpers directly --
# the injectable public seams (process_snapshot/terminate_process) are what
# reap_stale_frp_visitors's OWN tests below exercise; these prove the
# classification logic itself in isolation. reportPrivateUsage/SLF001 are
# suppressed throughout this section by design, not oversight.


def test_candidate_requires_the_visitor_config_dir_marker() -> None:
    record = ProcessRecord(pid=100, parent_pid=None, cmdline=f"{FRPC_BIN} -c /tmp/unrelated/x.toml")
    is_candidate = reconciliation._is_stale_visitor_candidate(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        record, frpc_bin=FRPC_BIN
    )
    assert is_candidate is False


def test_candidate_requires_the_configured_binary() -> None:
    record = ProcessRecord(
        pid=100, parent_pid=None, cmdline=_visitor_cmdline(frpc_bin="some-other-tool")
    )
    is_candidate = reconciliation._is_stale_visitor_candidate(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        record, frpc_bin=FRPC_BIN
    )
    assert is_candidate is False


def test_candidate_matches_marker_and_binary_together() -> None:
    record = ProcessRecord(pid=100, parent_pid=None, cmdline=_visitor_cmdline())
    is_candidate = reconciliation._is_stale_visitor_candidate(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        record, frpc_bin=FRPC_BIN
    )
    assert is_candidate is True


def _command_names_binary(command: str, frpc_bin: str) -> bool:
    return reconciliation._command_names_binary(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        command, frpc_bin
    )


def test_command_names_binary_matches_an_unqualified_configured_binary() -> None:
    """A bare ``"frpc"`` (resolved through PATH) never shows up as an absolute
    path in the observed cmdline -- the bare-name fallback must still match.
    """
    command = 'c:/tools/frpc.exe -c "c:/users/x/temp/clio-relay-frp-visitor-1/frpc-visitor.toml"'
    assert _command_names_binary(command, "frpc") is True


def test_command_names_binary_matches_an_absolute_configured_binary() -> None:
    command = "/opt/frp/frpc -c /tmp/clio-relay-frp-visitor-1/frpc-visitor.toml"
    assert _command_names_binary(command, "/opt/frp/frpc") is True


def test_command_names_binary_normalizes_windows_path_separators() -> None:
    command = r'c:\tools\frpc.exe -c "c:\temp\clio-relay-frp-visitor-1\frpc-visitor.toml"'
    assert _command_names_binary(command, "C:/tools/frpc.exe") is True


def test_command_names_binary_refuses_a_different_deployments_binary() -> None:
    command = "/opt/other-frp/frpc -c /tmp/clio-relay-frp-visitor-1/frpc-visitor.toml"
    assert _command_names_binary(command, "/opt/frp/frpc") is False


# reap_stale_frp_visitors


def test_reap_stale_frp_visitors_reaps_the_one_true_orphan() -> None:
    """One orphan (parent gone), one unrelated process, one QUALIFIED-path
    different deployment (see ...reaps_a_bare_configured_binary for the
    bare/unqualified case) -- exactly the orphan is reaped.
    """
    configured_bin = "/opt/frp/frpc"
    snapshot = [
        ProcessRecord(
            pid=501, parent_pid=None, cmdline=_visitor_cmdline(frpc_bin=configured_bin)
        ),  # the orphan
        ProcessRecord(pid=502, parent_pid=1, cmdline="python -m something"),  # unrelated
        ProcessRecord(
            pid=503, parent_pid=None, cmdline=_visitor_cmdline(frpc_bin="/opt/other-frp/frpc")
        ),  # different deployment (different directory, same basename)
    ]
    reaped_calls: list[int] = []

    result = reap_stale_frp_visitors(
        frpc_bin=configured_bin,
        process_snapshot=lambda: snapshot,
        terminate_process=reaped_calls.append,
    )

    assert result == (501,)
    assert reaped_calls == [501]


def test_reap_stale_frp_visitors_reaps_a_bare_configured_binary() -> None:
    """The common configuration (``frpc_bin="frpc"``, PATH-resolved) still reaps."""
    snapshot = [
        ProcessRecord(pid=504, parent_pid=None, cmdline=_visitor_cmdline(frpc_bin=FRPC_BIN))
    ]

    result = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: snapshot,
        terminate_process=lambda _pid: None,
    )

    assert result == (504,)


def test_reap_stale_frp_visitors_never_touches_a_process_whose_parent_is_alive() -> None:
    """The acceptance bar iowarp/clio-relay#285 states explicitly: a live
    parent means a concurrent CLI still legitimately holds its visitor.
    """
    snapshot = [
        ProcessRecord(pid=42, parent_pid=None, cmdline=""),  # the "still-running CLI"
        ProcessRecord(pid=601, parent_pid=42, cmdline=_visitor_cmdline()),  # its live visitor
    ]
    reaped_calls: list[int] = []

    result = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: snapshot,
        terminate_process=reaped_calls.append,
    )

    assert result == ()
    assert reaped_calls == []


def test_reap_stale_frp_visitors_reaps_a_visitor_with_unknown_parent() -> None:
    """``parent_pid=None`` (inspection could not determine it) is still
    eligible to be REAPED -- only a KNOWN-alive parent protects a candidate.
    """
    snapshot = [ProcessRecord(pid=701, parent_pid=None, cmdline=_visitor_cmdline())]

    result = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: snapshot,
        terminate_process=lambda _pid: None,
    )

    assert result == (701,)


def test_reap_stale_frp_visitors_reaps_multiple_orphans_in_one_pass() -> None:
    snapshot = [
        ProcessRecord(
            pid=801,
            parent_pid=None,
            cmdline=_visitor_cmdline(config_dir="clio-relay-frp-visitor-a"),
        ),
        ProcessRecord(
            pid=802,
            parent_pid=None,
            cmdline=_visitor_cmdline(config_dir="clio-relay-frp-visitor-b"),
        ),
    ]

    result = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: snapshot,
        terminate_process=lambda _pid: None,
    )

    assert set(result) == {801, 802}


def test_reap_stale_frp_visitors_uses_the_real_snapshot_and_kill_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No injected seam at all: the production defaults are reachable/wired."""
    calls: list[int] = []
    monkeypatch.setattr(
        reconciliation,
        "default_process_snapshot",
        lambda: (ProcessRecord(pid=999, parent_pid=None, cmdline=_visitor_cmdline()),),
    )
    monkeypatch.setattr(reconciliation, "_terminate_pid", calls.append)

    result = reap_stale_frp_visitors(frpc_bin=FRPC_BIN)

    assert result == (999,)
    assert calls == [999]


# sweep_stale_visitor_config_dirs


def _backdate(path: Path, *, seconds_ago: float) -> None:
    stamp = time.time() - seconds_ago
    os.utime(path, (stamp, stamp))


def test_sweep_removes_an_empty_aged_matching_dir(tmp_path: Path) -> None:
    stale = tmp_path / "clio-relay-frp-visitor-stale"
    stale.mkdir()
    _backdate(stale, seconds_ago=7200)

    swept = sweep_stale_visitor_config_dirs(temp_root=tmp_path, max_age_seconds=3600)

    assert swept == 1
    assert not stale.exists()


def test_sweep_keeps_a_recent_matching_dir(tmp_path: Path) -> None:
    fresh = tmp_path / "clio-relay-frp-visitor-fresh"
    fresh.mkdir()

    swept = sweep_stale_visitor_config_dirs(temp_root=tmp_path, max_age_seconds=3600)

    assert swept == 0
    assert fresh.exists()


def test_sweep_never_removes_a_populated_aged_dir(tmp_path: Path) -> None:
    populated = tmp_path / "clio-relay-frp-visitor-live"
    populated.mkdir()
    (populated / "frpc-visitor.toml").write_text("still held", encoding="utf-8")
    _backdate(populated, seconds_ago=7200)

    swept = sweep_stale_visitor_config_dirs(temp_root=tmp_path, max_age_seconds=3600)

    assert swept == 0
    assert populated.exists()
    assert (populated / "frpc-visitor.toml").exists()


def test_sweep_never_touches_an_unrelated_aged_dir(tmp_path: Path) -> None:
    unrelated = tmp_path / "some-other-tempdir"
    unrelated.mkdir()
    _backdate(unrelated, seconds_ago=7200)

    swept = sweep_stale_visitor_config_dirs(temp_root=tmp_path, max_age_seconds=3600)

    assert swept == 0
    assert unrelated.exists()


def test_sweep_never_touches_a_file_matching_the_prefix(tmp_path: Path) -> None:
    """Only directories are ever candidates -- a same-prefixed FILE is not one."""
    stray_file = tmp_path / "clio-relay-frp-visitor-not-a-dir"
    stray_file.write_text("", encoding="utf-8")
    _backdate(stray_file, seconds_ago=7200)

    swept = sweep_stale_visitor_config_dirs(temp_root=tmp_path, max_age_seconds=3600)

    assert swept == 0
    assert stray_file.exists()


def test_sweep_counts_every_empty_aged_matching_dir(tmp_path: Path) -> None:
    for index in range(3):
        stale = tmp_path / f"clio-relay-frp-visitor-{index}"
        stale.mkdir()
        _backdate(stale, seconds_ago=7200)

    swept = sweep_stale_visitor_config_dirs(temp_root=tmp_path, max_age_seconds=3600)

    assert swept == 3


def test_sweep_returns_zero_for_an_unreadable_temp_root(tmp_path: Path) -> None:
    missing_root = tmp_path / "does-not-exist"

    swept = sweep_stale_visitor_config_dirs(temp_root=missing_root, max_age_seconds=3600)

    assert swept == 0


# default_process_snapshot dispatch + platform parsers


def test_default_process_snapshot_dispatches_to_windows_on_nt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(reconciliation.os, "name", "nt")
    sentinel = (ProcessRecord(pid=1, parent_pid=None, cmdline="x"),)
    monkeypatch.setattr(reconciliation, "_windows_process_snapshot", lambda: sentinel)

    assert reconciliation.default_process_snapshot() == sentinel


def test_default_process_snapshot_dispatches_to_posix_off_nt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(reconciliation.os, "name", "posix")
    sentinel = (ProcessRecord(pid=2, parent_pid=None, cmdline="y"),)
    monkeypatch.setattr(reconciliation, "_posix_process_snapshot", lambda: sentinel)

    assert reconciliation.default_process_snapshot() == sentinel


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


def _posix_process_snapshot(proc_root: Path) -> tuple[ProcessRecord, ...]:
    return reconciliation._posix_process_snapshot(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
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

    records = _posix_process_snapshot(proc_root)

    assert len(records) == 1
    record = records[0]
    assert record.pid == 111
    assert record.parent_pid == 222
    assert "clio-relay-frp-visitor-x" in record.cmdline


def test_posix_process_snapshot_skips_an_entry_that_vanishes_mid_read(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir(parents=True)
    (proc_root / "999").mkdir()  # no stat/cmdline files inside -- simulates a race

    assert _posix_process_snapshot(proc_root) == ()


def test_posix_process_snapshot_returns_empty_for_a_missing_root(tmp_path: Path) -> None:
    assert _posix_process_snapshot(tmp_path / "does-not-exist") == ()


class _FakeCompletedProcess:
    def __init__(self, *, returncode: int, stdout: str) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = ""


def _windows_process_snapshot() -> tuple[ProcessRecord, ...]:
    return reconciliation._windows_process_snapshot()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_windows_process_snapshot_parses_powershell_json(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = (
        '[{"ProcessId": 111, "ParentProcessId": 222, '
        '"CommandLine": "frpc -c C:/temp/clio-relay-frp-visitor-x/frpc-visitor.toml"},'
        '{"ProcessId": 0, "ParentProcessId": 0, "CommandLine": null}]'
    )

    def fake_run(*_args: object, **_kwargs: object) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(returncode=0, stdout=payload)

    monkeypatch.setattr(reconciliation.subprocess, "run", fake_run)

    records = _windows_process_snapshot()

    assert len(records) == 1  # the PID-0 sentinel row is discarded
    assert records[0].pid == 111
    assert records[0].parent_pid == 222
    assert "clio-relay-frp-visitor-x" in records[0].cmdline


@pytest.mark.parametrize(("returncode", "stdout"), [(1, ""), (0, "not json")])
def test_windows_process_snapshot_returns_empty_on_a_bad_result(
    monkeypatch: pytest.MonkeyPatch, returncode: int, stdout: str
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(returncode=returncode, stdout=stdout)

    monkeypatch.setattr(reconciliation.subprocess, "run", fake_run)

    assert _windows_process_snapshot() == ()


@pytest.mark.parametrize(
    "raised",
    [OSError("powershell not found"), subprocess.TimeoutExpired(cmd="powershell", timeout=20)],
)
def test_windows_process_snapshot_returns_empty_when_the_call_itself_fails(
    monkeypatch: pytest.MonkeyPatch, raised: Exception
) -> None:
    def _raise(*_args: object, **_kwargs: object) -> None:
        raise raised

    monkeypatch.setattr(reconciliation.subprocess, "run", _raise)

    assert _windows_process_snapshot() == ()


# reconcile_stale_frp_visitors (the orchestrator frp_transport.py calls)


def test_reconcile_composes_reap_and_sweep(tmp_path: Path) -> None:
    stale = tmp_path / "clio-relay-frp-visitor-stale"
    stale.mkdir()
    _backdate(stale, seconds_ago=7200)
    snapshot = [ProcessRecord(pid=321, parent_pid=None, cmdline=_visitor_cmdline())]

    result = reconcile_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: snapshot,
        terminate_process=lambda _pid: None,
        temp_root=tmp_path,
        max_config_dir_age_seconds=3600,
    )

    assert result == ReconciliationResult(reaped_pids=(321,), swept_config_dirs=1)


def test_reconcile_never_raises_when_reaping_fails_unexpectedly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Best-effort cleanup must never block a new visitor's own spawn (#285)."""

    def _raise(**_kwargs: object) -> tuple[int, ...]:
        raise RuntimeError("simulated: process-table inspection blew up")

    monkeypatch.setattr(reconciliation, "reap_stale_frp_visitors", _raise)

    result = reconcile_stale_frp_visitors(frpc_bin=FRPC_BIN)

    assert result == ReconciliationResult(reaped_pids=(), swept_config_dirs=0)


def test_reconcile_never_raises_when_sweeping_fails_unexpectedly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(**_kwargs: object) -> int:
        raise RuntimeError("simulated: sweep blew up")

    monkeypatch.setattr(reconciliation, "sweep_stale_visitor_config_dirs", _raise)

    result = reconcile_stale_frp_visitors(frpc_bin=FRPC_BIN)

    assert result == ReconciliationResult(reaped_pids=(), swept_config_dirs=0)

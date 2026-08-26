"""Classification, reap (D1 pid-reuse re-verify, D3 secret removal), and sweep
unit coverage for the stale-frpc-visitor reconciliation pass (#285). Every
test uses FAKE ``ProcessRecord`` rows -- never a real OS process.

Split to stay under the file-size sweet spot: cross-platform snapshot/lookup
parsing, D2's typed-skip-reason coverage, and the
``reconcile_stale_frp_visitors`` orchestrator (incl. D8's once-per-process
sweep gate) live in ``test_frp_visitor_reconciliation_platform.py`` instead.
Integration tests proving the wiring into an actual visitor spawn live in
``test_frp_transport_dials.py``.
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
    ProcessSnapshot,
    ReapOutcome,
    reap_stale_frp_visitors,
    sweep_stale_visitor_config_dirs,
)

FRPC_BIN = "frpc"


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


# _extract_config_path / _tokenize_command (D3)


def _extract_config_path(command: str) -> str | None:
    return reconciliation._extract_config_path(command)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_extract_config_path_reads_the_dash_c_argument() -> None:
    assert (
        _extract_config_path(_visitor_cmdline())
        == "/tmp/clio-relay-frp-visitor-abc123/frpc-visitor.toml"
    )


def test_extract_config_path_handles_a_quoted_windows_path() -> None:
    command = 'frpc.exe -c "C:/temp/clio-relay-frp-visitor-x/frpc-visitor.toml"'
    assert _extract_config_path(command) == "C:/temp/clio-relay-frp-visitor-x/frpc-visitor.toml"


def test_extract_config_path_returns_none_without_a_dash_c_token() -> None:
    assert _extract_config_path("frpc --version") is None


# reap_stale_frp_visitors


def test_reap_stale_frp_visitors_reaps_the_one_true_orphan() -> None:
    """One orphan (parent gone), one unrelated process, one QUALIFIED-path
    different deployment (see ...reaps_a_bare_configured_binary for the
    bare/unqualified case) -- exactly the orphan is reaped.
    """
    configured_bin = "/opt/frp/frpc"
    orphan = ProcessRecord(
        pid=501, parent_pid=None, cmdline=_visitor_cmdline(frpc_bin=configured_bin)
    )
    snapshot = [
        orphan,
        ProcessRecord(pid=502, parent_pid=1, cmdline="python -m something"),  # unrelated
        ProcessRecord(
            pid=503, parent_pid=None, cmdline=_visitor_cmdline(frpc_bin="/opt/other-frp/frpc")
        ),  # different deployment (different directory, same basename)
    ]
    reaped_calls: list[int] = []

    outcome = reap_stale_frp_visitors(
        frpc_bin=configured_bin,
        process_snapshot=lambda: _snapshot(snapshot),
        terminate_process=reaped_calls.append,
        process_lookup=_lookup_returning(orphan),
    )

    assert outcome == ReapOutcome(reaped_pids=(501,))
    assert reaped_calls == [501]


def test_reap_stale_frp_visitors_reaps_a_bare_configured_binary() -> None:
    """The common configuration (``frpc_bin="frpc"``, PATH-resolved) still reaps."""
    orphan = ProcessRecord(pid=504, parent_pid=None, cmdline=_visitor_cmdline(frpc_bin=FRPC_BIN))

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([orphan]),
        terminate_process=lambda _pid: None,
        process_lookup=_lookup_returning(orphan),
    )

    assert outcome.reaped_pids == (504,)


def test_reap_stale_frp_visitors_never_touches_a_process_whose_parent_is_alive() -> None:
    """The acceptance bar iowarp/clio-relay#285 states explicitly: a live
    parent means a concurrent CLI still legitimately holds its visitor.
    """
    snapshot = [
        ProcessRecord(pid=42, parent_pid=None, cmdline=""),  # the "still-running CLI"
        ProcessRecord(pid=601, parent_pid=42, cmdline=_visitor_cmdline()),  # its live visitor
    ]
    lookup_calls: list[int] = []

    def lookup(pid: int) -> ProcessRecord | None:
        lookup_calls.append(pid)
        return None

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot(snapshot),
        terminate_process=lambda _pid: pytest.fail("must never be called"),
        process_lookup=lookup,
    )

    assert outcome.reaped_pids == ()
    # The live-parent short-circuit runs BEFORE the D1 re-verify lookup.
    assert lookup_calls == []


def test_reap_stale_frp_visitors_reaps_a_visitor_with_unknown_parent() -> None:
    """``parent_pid=None`` (inspection could not determine it) is still
    eligible to be REAPED -- only a KNOWN-alive parent protects a candidate.
    """
    orphan = ProcessRecord(pid=701, parent_pid=None, cmdline=_visitor_cmdline())

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([orphan]),
        terminate_process=lambda _pid: None,
        process_lookup=_lookup_returning(orphan),
    )

    assert outcome.reaped_pids == (701,)


def test_reap_stale_frp_visitors_reaps_multiple_orphans_in_one_pass() -> None:
    orphan_a = ProcessRecord(
        pid=801, parent_pid=None, cmdline=_visitor_cmdline(config_dir="clio-relay-frp-visitor-a")
    )
    orphan_b = ProcessRecord(
        pid=802, parent_pid=None, cmdline=_visitor_cmdline(config_dir="clio-relay-frp-visitor-b")
    )
    by_pid = {801: orphan_a, 802: orphan_b}

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([orphan_a, orphan_b]),
        terminate_process=lambda _pid: None,
        process_lookup=lambda pid: by_pid.get(pid),
    )

    assert set(outcome.reaped_pids) == {801, 802}


def test_reap_stale_frp_visitors_uses_the_real_snapshot_and_kill_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No injected seam at all: the production defaults are reachable/wired."""
    orphan = ProcessRecord(pid=999, parent_pid=None, cmdline=_visitor_cmdline())
    calls: list[int] = []

    def fake_lookup(_pid: int) -> ProcessRecord | None:
        return orphan

    monkeypatch.setattr(reconciliation, "default_process_snapshot", lambda: _snapshot([orphan]))
    monkeypatch.setattr(reconciliation, "default_single_process_lookup", fake_lookup)
    monkeypatch.setattr(reconciliation, "_terminate_pid", calls.append)

    outcome = reap_stale_frp_visitors(frpc_bin=FRPC_BIN)

    assert outcome.reaped_pids == (999,)
    assert calls == [999]


# D1: pid-reuse TOCTOU -- the fresh, immediately-pre-kill re-verify.


def test_reap_refuses_to_kill_a_pid_reused_by_a_non_matching_process() -> None:
    """The adversarial-review repro: a stale snapshot row names pid 501 as an
    orphan candidate, but a FRESH read of that exact pid, taken immediately
    before the kill, shows an unrelated process now holds it (the original
    visitor already exited on its own and the OS recycled the pid) -- the
    kill must be refused.
    """
    stale_row = ProcessRecord(pid=501, parent_pid=None, cmdline=_visitor_cmdline())
    innocent_now_at_that_pid = ProcessRecord(
        pid=501, parent_pid=1, cmdline="notepad.exe some_file.txt"
    )
    reaped_calls: list[int] = []

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([stale_row]),
        terminate_process=reaped_calls.append,
        process_lookup=_lookup_returning(innocent_now_at_that_pid),
    )

    assert outcome.reaped_pids == ()
    assert reaped_calls == []


def test_reap_kills_when_the_fresh_reverify_still_matches() -> None:
    """The ordinary case: the fresh re-read still describes the same orphan."""
    stale_row = ProcessRecord(pid=502, parent_pid=None, cmdline=_visitor_cmdline())
    fresh_row = ProcessRecord(pid=502, parent_pid=None, cmdline=_visitor_cmdline())
    reaped_calls: list[int] = []

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([stale_row]),
        terminate_process=reaped_calls.append,
        process_lookup=_lookup_returning(fresh_row),
    )

    assert outcome.reaped_pids == (502,)
    assert reaped_calls == [502]


def test_reap_refuses_to_kill_a_pid_that_already_exited_before_the_reverify() -> None:
    """The fresh lookup finding nothing (pid gone) is also a refusal to kill --
    there is nothing left to kill, and no config-dir removal is attempted
    from a lookup that returned None (the sweep is the safety net there).
    """
    stale_row = ProcessRecord(pid=503, parent_pid=None, cmdline=_visitor_cmdline())
    reaped_calls: list[int] = []

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([stale_row]),
        terminate_process=reaped_calls.append,
        process_lookup=_lookup_returning(None),
    )

    assert outcome.reaped_pids == ()
    assert reaped_calls == []


def test_terminate_pid_windows_kill_never_passes_slash_t(monkeypatch: pytest.MonkeyPatch) -> None:
    """D1: frpc spawns no children -- ``/T`` only ever adds blast radius
    (the adversarial review's own repro: a stale-row kill amplified by
    ``/T`` took an unrelated child process down too).
    """
    monkeypatch.setattr(reconciliation.os, "name", "nt")
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(reconciliation.subprocess, "run", fake_run)

    reconciliation._terminate_pid(4242)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert len(calls) == 1
    assert calls[0] == ["taskkill", "/PID", "4242", "/F"]
    assert "/T" not in calls[0]


# D3: reap-time secret-config-dir removal.


def test_reap_removes_the_reaped_visitors_own_config_dir(tmp_path: Path) -> None:
    config_dir = tmp_path / "clio-relay-frp-visitor-xyz"
    config_dir.mkdir()
    config_file = config_dir / "frpc-visitor.toml"
    config_file.write_text("secretKey = 'plaintext-secret'", encoding="utf-8")
    cmdline = f"{FRPC_BIN} -c {config_file}"
    orphan = ProcessRecord(pid=601, parent_pid=None, cmdline=cmdline)

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([orphan]),
        terminate_process=lambda _pid: None,
        process_lookup=_lookup_returning(orphan),
    )

    assert outcome.reaped_pids == (601,)
    assert not config_dir.exists()


def test_reap_never_removes_a_config_dir_when_the_reverify_refuses_the_kill(tmp_path: Path) -> None:
    """A pid-reuse refusal (D1) must not remove anything either."""
    config_dir = tmp_path / "clio-relay-frp-visitor-still-here"
    config_dir.mkdir()
    config_file = config_dir / "frpc-visitor.toml"
    config_file.write_text("secretKey = 'plaintext-secret'", encoding="utf-8")
    stale_row = ProcessRecord(pid=602, parent_pid=None, cmdline=f"{FRPC_BIN} -c {config_file}")
    innocent = ProcessRecord(pid=602, parent_pid=1, cmdline="notepad.exe")

    outcome = reap_stale_frp_visitors(
        frpc_bin=FRPC_BIN,
        process_snapshot=lambda: _snapshot([stale_row]),
        terminate_process=lambda _pid: None,
        process_lookup=_lookup_returning(innocent),
    )

    assert outcome.reaped_pids == ()
    assert config_dir.exists()
    assert config_file.exists()


def test_remove_visitor_config_dir_refuses_a_path_without_the_naming_convention(
    tmp_path: Path,
) -> None:
    """Never rm -rf an unrelated dir: the resolved parent must itself carry the prefix."""
    unrelated_dir = tmp_path / "not-a-visitor-dir"
    unrelated_dir.mkdir()
    unrelated_file = unrelated_dir / "frpc-visitor.toml"
    unrelated_file.write_text("x", encoding="utf-8")

    reconciliation._remove_visitor_config_dir(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        f"{FRPC_BIN} -c {unrelated_file}"
    )

    assert unrelated_dir.exists()


def test_remove_visitor_config_dir_is_a_no_op_without_a_dash_c_argument() -> None:
    # Must not raise -- best-effort, and there is nothing to extract.
    reconciliation._remove_visitor_config_dir("frpc --version")  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


# sweep_stale_visitor_config_dirs (D3: now removes non-empty dirs too)


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


def test_sweep_removes_a_populated_aged_dir_secret_and_all(tmp_path: Path) -> None:
    """D3 adversarial-review fix: this is the crash-path leak itself -- a
    non-empty, aged visitor dir must be removed, secret file included, not
    preserved. (Sabotage twin of the earlier, INCORRECT expectation this
    test replaces: an emptiness guard here is exactly what let the secret
    persist indefinitely.)
    """
    populated = tmp_path / "clio-relay-frp-visitor-live"
    populated.mkdir()
    secret_file = populated / "frpc-visitor.toml"
    secret_file.write_text("secretKey = 'plaintext-secret'", encoding="utf-8")
    _backdate(populated, seconds_ago=7200)

    swept = sweep_stale_visitor_config_dirs(temp_root=tmp_path, max_age_seconds=3600)

    assert swept == 1
    assert not populated.exists()
    assert not secret_file.exists()


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

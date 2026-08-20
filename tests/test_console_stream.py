"""Tests for the #259 console log stream substrate (clio_relay.console_stream)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from clio_relay.console_stream import (
    CONSOLE_STREAM,
    ConsoleLiveTailer,
    ConsoleTailUnavailable,
    console_tailer_for_mcp_call,
    flush_terminal_console,
    flush_terminal_console_from_path,
    newest_execution_dir,
    resolve_jarvis_shared_dir,
)
from clio_relay.models import JobKind, McpCallSpec, RelayJob
from clio_relay.spool import JobSpool


def _job(*, tool: str = "jarvis_run", arguments: dict[str, object] | None = None) -> RelayJob:
    return RelayJob(
        cluster="test-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server="jarvis-mcp",
            tool=tool,
            arguments=arguments or {"pipeline_id": "lammps-melt"},
        ),
        idempotency_key=f"console-{tool}",
    )


def _spool(tmp_path: Path, job: RelayJob | None = None) -> JobSpool:
    spool = JobSpool(tmp_path / "spool", job or _job())
    spool.initialize()
    return spool


def _write_jarvis_config(root: Path, *, shared_dir: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "jarvis_config.yaml").write_text(
        json.dumps(
            {
                "config_dir": str(root),
                "private_dir": str(root / "private"),
                "shared_dir": str(shared_dir),
            }
        ),
        encoding="utf-8",
    )


def _execution_dir(shared_dir: Path, pipeline_id: str, execution_id: str, *, mtime: float) -> Path:
    execution_root = shared_dir / pipeline_id / "executions" / execution_id
    execution_root.mkdir(parents=True)
    os.utime(execution_root, (mtime, mtime))
    return execution_root


def _write(path: Path, text: str) -> None:
    """Write exact bytes -- ``Path.write_text`` translates ``\\n`` to the
    platform line ending, which would corrupt the byte-exact assertions
    below on Windows."""
    path.write_bytes(text.encode("utf-8"))


def _append(path: Path, text: str) -> None:
    with path.open("ab") as handle:
        handle.write(text.encode("utf-8"))


# --- resolve_jarvis_shared_dir -------------------------------------------------


def test_resolve_jarvis_shared_dir_reads_the_configured_root(tmp_path: Path) -> None:
    jarvis_root = tmp_path / "jarvis-root"
    shared_dir = tmp_path / "shared"
    _write_jarvis_config(jarvis_root, shared_dir=shared_dir)

    resolved = resolve_jarvis_shared_dir({"JARVIS_ROOT": str(jarvis_root)})

    assert resolved == shared_dir


def test_resolve_jarvis_shared_dir_reports_typed_reason_when_config_missing(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConsoleTailUnavailable) as caught:
        resolve_jarvis_shared_dir({"JARVIS_ROOT": str(tmp_path / "never-bootstrapped")})

    assert caught.value.reason == "jarvis_config_missing"


def test_resolve_jarvis_shared_dir_reports_typed_reason_when_shared_dir_omitted(
    tmp_path: Path,
) -> None:
    jarvis_root = tmp_path / "jarvis-root"
    jarvis_root.mkdir()
    (jarvis_root / "jarvis_config.yaml").write_text(
        json.dumps({"config_dir": str(jarvis_root)}),
        encoding="utf-8",
    )

    with pytest.raises(ConsoleTailUnavailable) as caught:
        resolve_jarvis_shared_dir({"JARVIS_ROOT": str(jarvis_root)})

    assert caught.value.reason == "jarvis_shared_dir_undeclared"


def test_resolve_jarvis_shared_dir_reports_typed_reason_for_invalid_yaml(tmp_path: Path) -> None:
    jarvis_root = tmp_path / "jarvis-root"
    jarvis_root.mkdir()
    (jarvis_root / "jarvis_config.yaml").write_text("not: [valid: yaml", encoding="utf-8")

    with pytest.raises(ConsoleTailUnavailable) as caught:
        resolve_jarvis_shared_dir({"JARVIS_ROOT": str(jarvis_root)})

    assert caught.value.reason == "jarvis_config_unreadable"


def test_resolve_jarvis_shared_dir_falls_back_to_home_dot_ppi_jarvis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_home = tmp_path / "home"
    shared_dir = tmp_path / "shared"
    _write_jarvis_config(fake_home / ".ppi-jarvis", shared_dir=shared_dir)
    monkeypatch.setattr(Path, "home", lambda: fake_home)

    resolved = resolve_jarvis_shared_dir({})

    assert resolved == shared_dir


# --- newest_execution_dir -------------------------------------------------------


def test_newest_execution_dir_picks_the_most_recently_created_directory(
    tmp_path: Path,
) -> None:
    shared_dir = tmp_path / "shared"
    older = _execution_dir(shared_dir, "lammps-melt", "exec-older", mtime=1_000)
    newer = _execution_dir(shared_dir, "lammps-melt", "exec-newer", mtime=2_000)
    del older

    located = newest_execution_dir(shared_dir, "lammps-melt")

    assert located == newer


def test_newest_execution_dir_returns_none_when_not_created_yet(tmp_path: Path) -> None:
    assert newest_execution_dir(tmp_path / "shared", "lammps-melt") is None


def test_newest_execution_dir_rejects_an_unsafe_pipeline_id(tmp_path: Path) -> None:
    with pytest.raises(ConsoleTailUnavailable) as caught:
        newest_execution_dir(tmp_path / "shared", "../escape")

    assert caught.value.reason == "pipeline_id_invalid"


# --- ConsoleLiveTailer -----------------------------------------------------------


def test_console_live_tailer_tails_a_growing_stdout_log_across_polls(tmp_path: Path) -> None:
    """A fake execution dir with a growing stdout.log drives the tailer."""
    jarvis_root = tmp_path / "jarvis-root"
    shared_dir = tmp_path / "shared"
    _write_jarvis_config(jarvis_root, shared_dir=shared_dir)
    execution_root = _execution_dir(shared_dir, "lammps-melt", "exec-1", mtime=1_000)
    stdout_log = execution_root / "stdout.log"
    _write(stdout_log, "Step 0 Temp 300.0\n")

    spool = _spool(tmp_path)
    tailer = ConsoleLiveTailer(
        spool=spool,
        pipeline_id="lammps-melt",
        env={"JARVIS_ROOT": str(jarvis_root)},
    )

    first = tailer.poll(now=0.0)
    assert first.appended is True
    assert first.reason is None

    # A second poll inside the throttle window is a cheap no-op.
    idle = tailer.poll(now=0.5)
    assert idle.appended is False
    assert idle.reason is None

    _append(stdout_log, "Step 1 Temp 301.2\n")

    second = tailer.poll(now=3.0)
    assert second.appended is True

    text, _next_offset, eof = spool.read_log(CONSOLE_STREAM)
    assert text == "Step 0 Temp 300.0\nStep 1 Temp 301.2\n"
    assert eof is True


def test_console_live_tailer_locks_onto_the_first_execution_dir_and_never_switches(
    tmp_path: Path,
) -> None:
    jarvis_root = tmp_path / "jarvis-root"
    shared_dir = tmp_path / "shared"
    _write_jarvis_config(jarvis_root, shared_dir=shared_dir)
    first_execution = _execution_dir(shared_dir, "lammps-melt", "exec-first", mtime=1_000)
    _write(first_execution / "stdout.log", "from first\n")

    spool = _spool(tmp_path)
    tailer = ConsoleLiveTailer(
        spool=spool,
        pipeline_id="lammps-melt",
        env={"JARVIS_ROOT": str(jarvis_root)},
    )
    tailer.poll(now=0.0)
    assert tailer.execution_root == first_execution

    # A newer sibling execution directory appears (e.g. a second job reusing
    # the same pipeline id concurrently) -- the tailer must not jump to it.
    second_execution = _execution_dir(shared_dir, "lammps-melt", "exec-second", mtime=2_000)
    _write(second_execution / "stdout.log", "from second\n")

    tailer.poll(now=3.0)

    assert tailer.execution_root == first_execution
    text, _, _ = spool.read_log(CONSOLE_STREAM)
    assert text == "from first\n"
    assert "from second" not in text


def test_console_live_tailer_reports_a_typed_reason_once_and_never_raises(
    tmp_path: Path,
) -> None:
    """A tail failure (here: JARVIS never bootstrapped) is a typed, deduplicated
    reason -- the job itself must never fail because console tailing failed."""
    spool = _spool(tmp_path)
    tailer = ConsoleLiveTailer(
        spool=spool,
        pipeline_id="lammps-melt",
        env={"JARVIS_ROOT": str(tmp_path / "never-bootstrapped")},
    )

    first = tailer.poll(now=0.0)
    assert first.reason == "jarvis_config_missing"
    assert first.message is not None

    second = tailer.poll(now=3.0)
    assert second.reason is None  # deduplicated, not re-reported every tick

    third = tailer.poll(now=6.0)
    assert third.reason is None


def test_console_live_tailer_reports_a_typed_reason_when_stdout_log_vanishes(
    tmp_path: Path,
) -> None:
    jarvis_root = tmp_path / "jarvis-root"
    shared_dir = tmp_path / "shared"
    _write_jarvis_config(jarvis_root, shared_dir=shared_dir)
    execution_root = _execution_dir(shared_dir, "lammps-melt", "exec-1", mtime=1_000)
    stdout_log = execution_root / "stdout.log"
    _write(stdout_log, "Step 0\n")

    spool = _spool(tmp_path)
    tailer = ConsoleLiveTailer(
        spool=spool,
        pipeline_id="lammps-melt",
        env={"JARVIS_ROOT": str(jarvis_root)},
    )
    tailer.poll(now=0.0)
    stdout_log.unlink()

    step = tailer.poll(now=3.0)

    assert step.reason == "stdout_log_unreadable"
    assert step.message is not None


def test_console_live_tailer_stops_after_the_spool_stream_quota_is_hit(
    tmp_path: Path,
) -> None:
    jarvis_root = tmp_path / "jarvis-root"
    shared_dir = tmp_path / "shared"
    _write_jarvis_config(jarvis_root, shared_dir=shared_dir)
    execution_root = _execution_dir(shared_dir, "lammps-melt", "exec-1", mtime=1_000)
    (execution_root / "stdout.log").write_text("x" * 100, encoding="utf-8")

    spool = JobSpool(
        tmp_path / "spool",
        _job(),
        max_log_bytes_per_stream=10,
        max_log_bytes_per_job=20,
    )
    spool.initialize()
    tailer = ConsoleLiveTailer(
        spool=spool,
        pipeline_id="lammps-melt",
        env={"JARVIS_ROOT": str(jarvis_root)},
    )

    first = tailer.poll(now=0.0)
    assert tailer.truncated is True
    assert first.reason == "truncated"
    assert first.message is not None

    # Further polls are cheap no-ops once truncated -- no repeated reads of a
    # firehose file, and definitely no repeated truncation events.
    again = tailer.poll(now=3.0)
    assert again.appended is False
    assert again.reason is None


def test_console_tailer_for_mcp_call_only_binds_the_jarvis_run_tool(tmp_path: Path) -> None:
    spool = _spool(tmp_path)

    run_spec = McpCallSpec(
        server="jarvis-mcp",
        tool="jarvis_run",
        arguments={"pipeline_id": "lammps-melt"},
    )
    query_spec = McpCallSpec(
        server="jarvis-mcp",
        tool="jarvis_get_execution",
        arguments={"execution_id": "exec-1"},
    )
    no_pipeline_spec = McpCallSpec(server="jarvis-mcp", tool="jarvis_run", arguments={})

    assert console_tailer_for_mcp_call(run_spec, spool=spool) is not None
    assert console_tailer_for_mcp_call(query_spec, spool=spool) is None
    assert console_tailer_for_mcp_call(no_pipeline_spec, spool=spool) is None


# --- flush_terminal_console / flush_terminal_console_from_path -----------------


def _result_document(execution_root: Path) -> dict[str, object]:
    return {
        "structured_result": {
            "execution_id": "exec-1",
            "execution_record": {
                "terminal": True,
                "metadata": {"pipeline_snapshot_path": str(execution_root / "submit.sh")},
            },
        }
    }


def test_flush_terminal_console_flushes_the_full_log_when_no_tailer_ran(tmp_path: Path) -> None:
    execution_root = tmp_path / "execution"
    execution_root.mkdir()
    _write(execution_root / "stdout.log", "Step 0\nStep 1\n")
    spool = _spool(tmp_path)

    outcome = flush_terminal_console(spool, _result_document(execution_root), tailer=None)

    assert outcome.reason is None
    assert outcome.appended_bytes == len("Step 0\nStep 1\n")
    text, _, eof = spool.read_log(CONSOLE_STREAM)
    assert text == "Step 0\nStep 1\n"
    assert eof is True


def test_flush_terminal_console_reports_a_truncated_reason_when_the_quota_is_hit(
    tmp_path: Path,
) -> None:
    execution_root = tmp_path / "execution"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_text("x" * 100, encoding="utf-8")
    spool = JobSpool(
        tmp_path / "spool",
        _job(),
        max_log_bytes_per_stream=10,
        max_log_bytes_per_job=20,
    )
    spool.initialize()

    outcome = flush_terminal_console(spool, _result_document(execution_root), tailer=None)

    assert outcome.reason == "truncated"
    assert outcome.message is not None
    assert outcome.appended_bytes == 10


def test_flush_terminal_console_appends_only_the_remainder_after_a_live_tail(
    tmp_path: Path,
) -> None:
    """The terminal flush must not duplicate bytes the live tailer already sent."""
    jarvis_root = tmp_path / "jarvis-root"
    shared_dir = tmp_path / "shared"
    _write_jarvis_config(jarvis_root, shared_dir=shared_dir)
    execution_root = _execution_dir(shared_dir, "lammps-melt", "exec-1", mtime=1_000)
    stdout_log = execution_root / "stdout.log"
    _write(stdout_log, "Step 0\n")

    spool = _spool(tmp_path)
    tailer = ConsoleLiveTailer(
        spool=spool,
        pipeline_id="lammps-melt",
        env={"JARVIS_ROOT": str(jarvis_root)},
    )
    tailer.poll(now=0.0)  # tails "Step 0\n" live

    _append(stdout_log, "Step 1\nStep 2\n")

    outcome = flush_terminal_console(spool, _result_document(execution_root), tailer=tailer)

    assert outcome.reason is None
    text, _, eof = spool.read_log(CONSOLE_STREAM)
    assert text == "Step 0\nStep 1\nStep 2\n"
    assert eof is True


def test_flush_terminal_console_reflushes_and_reports_a_mismatch(tmp_path: Path) -> None:
    """A tailer that followed a different execution root (the rare concurrent
    pipeline-reuse case) gets a typed mismatch reason and a full re-flush from
    the AUTHORITATIVE root, never the wrong content."""
    followed_root = tmp_path / "followed"
    followed_root.mkdir()
    _write(followed_root / "stdout.log", "wrong execution\n")
    authoritative_root = tmp_path / "authoritative"
    authoritative_root.mkdir()
    _write(authoritative_root / "stdout.log", "right execution\n")

    spool = _spool(tmp_path)
    tailer = ConsoleLiveTailer(spool=spool, pipeline_id="lammps-melt")
    tailer.execution_root = followed_root
    tailer.offset = 0

    outcome = flush_terminal_console(spool, _result_document(authoritative_root), tailer=tailer)

    assert outcome.reason == "console_live_tail_execution_mismatch"
    assert outcome.message is not None
    text, _, _ = spool.read_log(CONSOLE_STREAM)
    assert text == "right execution\n"


def test_flush_terminal_console_reports_a_typed_reason_when_execution_root_is_missing(
    tmp_path: Path,
) -> None:
    spool = _spool(tmp_path)
    document = {
        "structured_result": {
            "execution_id": "exec-1",
            "execution_record": {"terminal": True, "metadata": {}},
        }
    }

    outcome = flush_terminal_console(spool, document, tailer=None)

    assert outcome.reason == "execution_root_undeclared"
    assert outcome.appended_bytes == 0


def test_flush_terminal_console_is_a_quiet_noop_for_non_jarvis_run_results(
    tmp_path: Path,
) -> None:
    """A tools/list or jarvis_get_execution result has no ``execution_record``
    of its own application to flush -- an empty, non-error outcome."""
    spool = _spool(tmp_path)

    outcome = flush_terminal_console(spool, {"structured_result": {}}, tailer=None)

    assert outcome.appended_bytes == 0
    assert outcome.reason is None
    assert outcome.message is None


def test_flush_terminal_console_from_path_reports_a_typed_reason_for_invalid_json(
    tmp_path: Path,
) -> None:
    spool = _spool(tmp_path)
    result_path = spool.path / "mcp-result.json"
    result_path.write_text("not json", encoding="utf-8")

    outcome = flush_terminal_console_from_path(spool, result_path, tailer=None)

    assert outcome.reason == "mcp_result_unreadable"


def test_flush_terminal_console_from_path_reads_a_real_result_document(tmp_path: Path) -> None:
    execution_root = tmp_path / "execution"
    execution_root.mkdir()
    _write(execution_root / "stdout.log", "Step 0\n")
    spool = _spool(tmp_path)
    result_path = spool.path / "mcp-result.json"
    result_path.write_text(
        json.dumps(_result_document(execution_root)),
        encoding="utf-8",
    )

    outcome = flush_terminal_console_from_path(spool, result_path, tailer=None)

    assert outcome.reason is None
    text, _, _ = spool.read_log(CONSOLE_STREAM)
    assert text == "Step 0\n"


# --- sabotage twin: stdout must never carry application output -----------------


def test_console_stream_never_mixes_with_the_stdout_stream(tmp_path: Path) -> None:
    """The stdout stream must carry ONLY process stdio (the MCP jsonrpc wire
    for a jarvis-backed mcp_call job), never application output -- even once
    a job also writes to the console stream."""
    spool = _spool(tmp_path)

    spool.append_stdout('{"jsonrpc": "2.0", "id": 1, "result": {}}\n')
    spool.append_log(CONSOLE_STREAM, "Step 0 Temp 300.0\n")
    spool.append_stdout('{"jsonrpc": "2.0", "method": "notifications/progress"}\n')

    stdout_text, _, _ = spool.read_log("stdout")
    console_text, _, _ = spool.read_log(CONSOLE_STREAM)

    assert "Step 0" not in stdout_text
    assert "jsonrpc" not in console_text
    assert stdout_text == (
        '{"jsonrpc": "2.0", "id": 1, "result": {}}\n'
        '{"jsonrpc": "2.0", "method": "notifications/progress"}\n'
    )
    assert console_text == "Step 0 Temp 300.0\n"
    assert (spool.path / "stdout.log").read_bytes() != (spool.path / "console.log").read_bytes()

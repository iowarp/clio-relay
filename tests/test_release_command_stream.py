"""Tests for the live-streaming, bounded subprocess runner (clio-relay#275)."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from clio_relay import release_command_stream as release_command_stream_module
from clio_relay.release_command_stream import (
    TIMEOUT_RETURNCODE,
    StreamedCommandResult,
    run_streaming_command,
)


def _process_is_alive(pid: int) -> bool:
    """Return whether a process id is still alive, checked cross-platform."""
    if os.name == "nt":
        completed = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            check=False,
            text=True,
        )
        return str(pid) in completed.stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_for_pid_file(pid_file: Path, *, timeout_seconds: float = 10.0) -> int:
    """Poll for a pid a spawned grandchild wrote, without a fixed sleep."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if pid_file.is_file():
            content = pid_file.read_text(encoding="utf-8").strip()
            if content:
                return int(content)
        time.sleep(0.1)
    raise AssertionError("grandchild never recorded its pid")


def _wait_until_dead(pid: int, *, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while _process_is_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.2)
    assert not _process_is_alive(pid), f"process {pid} is still alive"


def _write_grandchild_holdopen_scripts(
    tmp_path: Path,
    *,
    parent_sleeps: bool,
) -> tuple[Path, Path, Path]:
    """Write a parent+grandchild pair proving whole-tree, not single-process,
    cleanup: the grandchild inherits and holds the parent's stdout/stderr
    pipe open (Python's default fd inheritance) regardless of whether the
    immediate parent itself keeps running. Returns (parent, grandchild,
    pid_file); paths travel as argv (never interpolated into generated
    source) so an arbitrary tmp_path -- including a Windows username
    producing a `\\U...`-shaped substring -- can never be misparsed as a
    string escape by either script.
    """
    pid_file = tmp_path / "grandchild.pid"
    grandchild_script = tmp_path / "grandchild.py"
    grandchild_script.write_text(
        "import os, sys, time\n"
        "with open(sys.argv[1], 'w', encoding='utf-8') as handle:\n"
        "    handle.write(str(os.getpid()))\n"
        "time.sleep(60)\n",
        encoding="utf-8",
    )
    parent_tail = "time.sleep(60)\n" if parent_sleeps else ""
    parent_script = tmp_path / "spawn_and_maybe_hang.py"
    parent_script.write_text(
        "import subprocess, sys, time\n"
        "subprocess.Popen([sys.executable, sys.argv[1], sys.argv[2]])\n"
        "print('spawned', flush=True)\n" + parent_tail,
        encoding="utf-8",
    )
    return parent_script, grandchild_script, pid_file


def test_run_streaming_command_echoes_lines_live_and_captures_full_output(
    tmp_path: Path,
) -> None:
    """Each line reaches the echo callback as it arrives, not only at exit."""
    script = tmp_path / "slow_printer.py"
    script.write_text(
        "import sys, time\n"
        "print('first', flush=True)\n"
        "time.sleep(1.0)\n"
        "print('second', flush=True)\n",
        encoding="utf-8",
    )
    echoed: list[tuple[str, float]] = []
    lock = threading.Lock()

    def echo(line: str) -> None:
        with lock:
            echoed.append((line, time.monotonic()))

    result = run_streaming_command(
        [sys.executable, str(script)],
        cwd=tmp_path,
        timeout_seconds=30,
        echo=echo,
    )

    assert result.timed_out is False
    assert result.returncode == 0
    # Trailing newline preserved -- byte-faithful to a plain .read() capture
    # (clio-relay#275 review D9).
    assert result.stdout == "first\nsecond\n"
    assert [line for line, _ in echoed] == ["first", "second"]
    # Assert the RELATIVE delta between the two echoed lines, not an absolute
    # time-from-process-start bound: the latter is proven flaky by cold
    # interpreter startup (clio-relay#275 review D4). The >= 0.9s gap between
    # "first" and "second" can only be explained by "second" arriving after
    # the child's own 1s sleep, which is only possible if lines are echoed
    # live rather than batched after the process exits.
    assert echoed[1][1] - echoed[0][1] >= 0.9


def test_run_streaming_command_captures_stdout_and_stderr_separately(
    tmp_path: Path,
) -> None:
    """Stdout and stderr are pumped independently, never merged."""
    result = run_streaming_command(
        [
            sys.executable,
            "-c",
            "import sys; print('out-line'); print('err-line', file=sys.stderr)",
        ],
        cwd=tmp_path,
        timeout_seconds=30,
    )

    assert result.timed_out is False
    assert result.returncode == 0
    assert result.stdout == "out-line\n"
    assert result.stderr == "err-line\n"


def test_run_streaming_command_transcript_omits_newline_when_source_has_none(
    tmp_path: Path,
) -> None:
    """A stream that never ends in a newline is reproduced without adding one."""
    result = run_streaming_command(
        [sys.executable, "-c", "import sys; sys.stdout.write('no-newline-here')"],
        cwd=tmp_path,
        timeout_seconds=30,
    )

    assert result.stdout == "no-newline-here"


def test_run_streaming_command_timeout_kills_process_tree_and_returns_typed_marker(
    tmp_path: Path,
) -> None:
    """A hanging descendant tree is fully killed and the marker names why."""
    parent_script, grandchild_script, pid_file = _write_grandchild_holdopen_scripts(
        tmp_path, parent_sleeps=True
    )

    start = time.monotonic()
    result = run_streaming_command(
        [sys.executable, str(parent_script), str(grandchild_script), str(pid_file)],
        cwd=tmp_path,
        timeout_seconds=1.0,
    )
    elapsed = time.monotonic() - start

    assert result.timed_out is True
    assert result.timeout_reason == "check_timeout after 1s"
    # The common case: the whole-tree kill is CONFIRMED, so it contributes no
    # additional typed degradation of its own.
    assert result.degradations == ()
    completed = result.to_completed_process()
    assert completed.returncode == TIMEOUT_RETURNCODE
    assert "check_timeout after 1s" in completed.stderr
    # Must return promptly -- proof the whole tree was reaped, not merely
    # abandoned in the background while this call blocked on it. Generous
    # margin: two interpreter spawns plus the kill/reap round-trip.
    assert elapsed < 20.0

    grandchild_pid = _wait_for_pid_file(pid_file)
    _wait_until_dead(grandchild_pid)


def test_run_streaming_command_base_exception_kills_tree_and_propagates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """clio-relay#275 review D2: any interrupt (e.g. Ctrl-C mid-gate), not
    only an explicit timeout, must still kill the whole tree before
    propagating -- proven regression: only TimeoutExpired was handled, so a
    KeyboardInterrupt during the wait leaked the child (and, via
    start_new_session, left it unreachable by the terminal's own SIGINT)."""
    parent_script, grandchild_script, pid_file = _write_grandchild_holdopen_scripts(
        tmp_path, parent_sleeps=True
    )
    original_wait: Callable[..., int] = subprocess.Popen.wait  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
    calls = {"count": 0}

    def wait_side_effect(self: subprocess.Popen[str], timeout: float | None = None) -> int:
        calls["count"] += 1
        if calls["count"] == 1:
            raise KeyboardInterrupt("simulated Ctrl-C mid-gate")
        return original_wait(self, timeout=timeout)

    monkeypatch.setattr(subprocess.Popen, "wait", wait_side_effect)

    with pytest.raises(KeyboardInterrupt):
        run_streaming_command(
            [sys.executable, str(parent_script), str(grandchild_script), str(pid_file)],
            cwd=tmp_path,
            timeout_seconds=30,
        )

    grandchild_pid = _wait_for_pid_file(pid_file)
    _wait_until_dead(grandchild_pid)


def test_run_streaming_command_echo_failure_is_caught_typed_and_never_fatal(
    tmp_path: Path,
) -> None:
    """clio-relay#275 review D3: a raising echo sink (BrokenPipeError on a
    closed console is realistic) must not kill the pump. Proven regression:
    an unguarded echo turned an rc=0 command into a truncated, uncaptured
    failure with no typed reason at all."""

    def failing_echo(line: str) -> None:
        del line
        raise BrokenPipeError("console closed")

    result = run_streaming_command(
        [sys.executable, "-c", "print('a'); print('b'); print('c')"],
        cwd=tmp_path,
        timeout_seconds=30,
        echo=failing_echo,
    )

    assert result.timed_out is False
    assert result.returncode == 0
    assert result.stdout == "a\nb\nc\n"
    assert any(reason.startswith("echo_failed: BrokenPipeError") for reason in result.degradations)
    completed = result.to_completed_process()
    # The command itself still succeeded -- an echo hiccup is a side-channel
    # degradation, not a fabricated command failure.
    assert completed.returncode == 0
    assert "echo_failed: BrokenPipeError" in completed.stderr


def test_run_streaming_command_orphan_pump_gets_typed_reason_and_tree_kill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """clio-relay#275 review D7: proven regression -- when the tracked
    process exits cleanly (rc=0) but an orphaned grandchild still holds the
    pipe open, the old code waited up to 20s (10+10 sequential joins) and
    then returned rc=0 with NO typed reason and the orphan still alive. The
    fix shares one shutdown deadline across both joins, records a typed
    orphan_descendant reason, and still attempts a tree-kill.

    On POSIX the kill reliably reaches the orphan too (process-group
    membership survives the direct child's own exit). On Windows it cannot:
    proven live that ``taskkill /T`` -- and even a live-process WMI query by
    ParentProcessId -- both go silent the instant the direct parent has
    already exited, even though the child is unambiguously still running.
    That is a genuine platform limitation (see release_command_stream.py's
    own module docstring), so on Windows this asserts the typed
    ``tree_kill_unreachable`` reason instead of the orphan's death.
    """
    monkeypatch.setattr(release_command_stream_module, "_SHUTDOWN_JOIN_TIMEOUT_SECONDS", 0.5)
    parent_script, grandchild_script, pid_file = _write_grandchild_holdopen_scripts(
        tmp_path, parent_sleeps=False
    )

    start = time.monotonic()
    result = run_streaming_command(
        [sys.executable, str(parent_script), str(grandchild_script), str(pid_file)],
        cwd=tmp_path,
        timeout_seconds=30,
    )
    elapsed = time.monotonic() - start

    assert result.timed_out is False
    assert result.returncode == 0
    assert any(reason.startswith("orphan_descendant:") for reason in result.degradations)
    # ONE shared deadline (0.5s here), not two sequential ones: this must
    # return in well under a second of shutdown waiting, not ~1s (2x0.5).
    assert elapsed < 10.0

    grandchild_pid = _wait_for_pid_file(pid_file)
    try:
        if os.name == "nt":
            assert any(
                reason.startswith("tree_kill_unreachable:") for reason in result.degradations
            )
        else:
            _wait_until_dead(grandchild_pid)
    finally:
        # Test hygiene: on Windows the assertion above proves the module
        # could not reach this orphan, so it must still be alive -- clean it
        # up directly rather than leaking a sleeping process past the test.
        subprocess.run(
            ["taskkill", "/PID", str(grandchild_pid), "/F"]
            if os.name == "nt"
            else ["kill", "-9", str(grandchild_pid)],
            capture_output=True,
            check=False,
        )


def test_run_streaming_command_rejects_non_positive_timeout(tmp_path: Path) -> None:
    """A zero or negative timeout is refused before any process is spawned."""
    with pytest.raises(ValueError, match="must be positive"):
        run_streaming_command(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            timeout_seconds=0,
        )


def test_run_streaming_command_decode_error_resilience(tmp_path: Path) -> None:
    """A bad byte on the wire cannot kill the pump; it is replaced, not fatal."""
    result = run_streaming_command(
        [
            sys.executable,
            "-c",
            "import sys; sys.stdout.buffer.write(b'before-\\xff\\xfe-after\\n'); "
            "sys.stdout.buffer.flush()",
        ],
        cwd=tmp_path,
        timeout_seconds=30,
    )

    assert result.timed_out is False
    assert result.returncode == 0
    assert "before-" in result.stdout
    assert "-after" in result.stdout


def test_streamed_command_result_to_completed_process_preserves_success() -> None:
    """A non-timed-out result converts to an ordinary CompletedProcess untouched."""
    result = StreamedCommandResult(
        command=["echo", "hi"],
        returncode=0,
        stdout="hi",
        stderr="",
        timed_out=False,
        timeout_reason=None,
    )

    completed = result.to_completed_process()

    assert completed.returncode == 0
    assert completed.stdout == "hi"
    assert completed.stderr == ""


def test_streamed_command_result_folds_every_degradation_into_stderr() -> None:
    """Every typed degradation reaches stderr, not just a timeout's own reason."""
    result = StreamedCommandResult(
        command=["echo", "hi"],
        returncode=0,
        stdout="hi",
        stderr="",
        timed_out=False,
        timeout_reason=None,
        degradations=("echo_failed: BrokenPipeError: closed", "orphan_descendant: still alive"),
    )

    completed = result.to_completed_process()

    assert completed.returncode == 0
    assert "echo_failed: BrokenPipeError: closed" in completed.stderr
    assert "orphan_descendant: still alive" in completed.stderr


def test_format_seconds_avoids_scientific_notation() -> None:
    """clio-relay#275 review D11: no `1e+07`-shaped argv value, ever."""
    from clio_relay.release_command_stream import format_seconds

    assert format_seconds(300.0) == "300"
    assert format_seconds(1.5) == "1.5"
    assert format_seconds(10_000_000.0) == "10000000"
    assert "e" not in format_seconds(10_000_000.0)

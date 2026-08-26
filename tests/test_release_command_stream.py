"""Tests for the live-streaming, bounded subprocess runner (clio-relay#275)."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from clio_relay.release_command_stream import (
    TIMEOUT_RETURNCODE,
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

    start = time.monotonic()
    result = run_streaming_command(
        [sys.executable, str(script)],
        cwd=tmp_path,
        timeout_seconds=30,
        echo=echo,
    )

    assert result.timed_out is False
    assert result.returncode == 0
    assert result.stdout == "first\nsecond"
    assert [line for line, _ in echoed] == ["first", "second"]
    # The first line must have been echoed well before the 1s sleep between
    # the two prints elapses -- proof of live delivery, not batch-at-exit.
    first_seen_after = echoed[0][1] - start
    second_seen_after = echoed[1][1] - start
    assert first_seen_after < 0.5
    assert second_seen_after >= 0.9


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
    assert result.stdout == "out-line"
    assert result.stderr == "err-line"


def test_run_streaming_command_timeout_kills_process_tree_and_returns_typed_marker(
    tmp_path: Path,
) -> None:
    """A hanging descendant tree is fully killed and the marker names why."""
    # Paths travel as argv elements (never interpolated into generated Python
    # source) so arbitrary pytest tmp_path content -- including a `\U...`
    # sequence a Windows username can produce -- can never be misparsed as a
    # string escape by either script.
    pid_file = tmp_path / "grandchild.pid"
    grandchild_script = tmp_path / "grandchild.py"
    grandchild_script.write_text(
        "import os, sys, time\n"
        "with open(sys.argv[1], 'w', encoding='utf-8') as handle:\n"
        "    handle.write(str(os.getpid()))\n"
        "time.sleep(60)\n",
        encoding="utf-8",
    )
    parent_script = tmp_path / "spawn_and_hang.py"
    parent_script.write_text(
        "import subprocess, sys, time\n"
        "subprocess.Popen([sys.executable, sys.argv[1], sys.argv[2]])\n"
        "print('spawned', flush=True)\n"
        "time.sleep(60)\n",
        encoding="utf-8",
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
    completed = result.to_completed_process()
    assert completed.returncode == TIMEOUT_RETURNCODE
    assert "check_timeout after 1s" in completed.stderr
    # Must return promptly -- proof the whole tree was reaped, not merely
    # abandoned in the background while this call blocked on it.
    assert elapsed < 15.0

    deadline = time.monotonic() + 10.0
    grandchild_pid: int | None = None
    while time.monotonic() < deadline:
        if pid_file.is_file():
            content = pid_file.read_text(encoding="utf-8").strip()
            if content:
                grandchild_pid = int(content)
                break
        time.sleep(0.1)
    assert grandchild_pid is not None, "grandchild never recorded its pid"

    deadline = time.monotonic() + 10.0
    while _process_is_alive(grandchild_pid) and time.monotonic() < deadline:
        time.sleep(0.2)
    assert not _process_is_alive(grandchild_pid), (
        "grandchild survived the whole-tree kill -- only the direct child was signaled"
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
    from clio_relay.release_command_stream import StreamedCommandResult

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

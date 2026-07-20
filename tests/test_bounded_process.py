"""Finite process-tree and output-bound tests for bootstrap probes."""

from __future__ import annotations

import sys
import time

import pytest

from clio_relay.bounded_process import (
    BoundedProcessOutputLimit,
    BoundedProcessTimeout,
    BoundedProcessTreeLeak,
    run_bounded_process,
)


def test_bounded_process_terminates_timed_out_descendant_tree() -> None:
    """A timeout kills both the direct command and its sleeping descendant."""
    source = """
import subprocess
import sys
import time

subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
time.sleep(60)
"""
    started = time.monotonic()

    with pytest.raises(BoundedProcessTimeout):
        run_bounded_process(
            [sys.executable, "-c", source],
            timeout_seconds=0.2,
            stdout_maximum_bytes=1024,
            stderr_maximum_bytes=1024,
        )

    assert time.monotonic() - started < 10


def test_bounded_process_rejects_descendant_holding_output_pipe() -> None:
    """A direct exit cannot leave a child retaining the captured pipe."""
    source = """
import subprocess
import sys

subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
"""
    started = time.monotonic()

    with pytest.raises(BoundedProcessTreeLeak):
        run_bounded_process(
            [sys.executable, "-c", source],
            timeout_seconds=5,
            stdout_maximum_bytes=1024,
            stderr_maximum_bytes=1024,
        )

    assert time.monotonic() - started < 10


def test_bounded_process_rejects_output_overflow_distinctly() -> None:
    """Output exhaustion has an explicit failure separate from exit status."""
    with pytest.raises(BoundedProcessOutputLimit, match="stdout"):
        run_bounded_process(
            [sys.executable, "-c", "print('x' * 10000)"],
            timeout_seconds=5,
            stdout_maximum_bytes=128,
            stderr_maximum_bytes=128,
        )


def test_bounded_process_rejects_stderr_overflow_distinctly() -> None:
    """Stderr is bounded independently and retains no unbounded diagnostics."""
    with pytest.raises(BoundedProcessOutputLimit, match="stderr"):
        run_bounded_process(
            [sys.executable, "-c", "import sys; sys.stderr.write('x' * 10000)"],
            timeout_seconds=5,
            stdout_maximum_bytes=128,
            stderr_maximum_bytes=128,
        )


def test_bounded_process_delivers_fixed_stdin_payload() -> None:
    """The gated child receives fixed input only after containment is ready."""
    result = run_bounded_process(
        [sys.executable, "-c", "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())"],
        input_bytes=b"owned-request\n",
        timeout_seconds=5,
        stdout_maximum_bytes=1024,
        stderr_maximum_bytes=1024,
    )

    assert result.stdout == "owned-request\n"

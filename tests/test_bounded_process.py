"""Finite process-tree and output-bound tests for bootstrap probes."""

from __future__ import annotations

import sys
import time
from hashlib import sha256

import pytest
from _pytest.monkeypatch import MonkeyPatch

import clio_relay.bounded_process as bounded_process
from clio_relay.bounded_process import (
    BoundedProcessError,
    BoundedProcessOutputLimit,
    BoundedProcessTimeout,
    BoundedProcessTreeLeak,
    run_bounded_process,
)
from clio_relay.process_containment import OwnedProcessSpawnError


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


def test_bounded_process_streams_fixed_stdin_beyond_broker_setup_limit() -> None:
    """Large fixed input streams after containment without base64 setup inflation."""
    payload = (b"clio-relay-sidecar-boundary\n" * 200_000)[: 5 * 1024 * 1024]
    expected = f"{len(payload)}:{sha256(payload).hexdigest()}"
    source = """
import hashlib
import sys

payload = sys.stdin.buffer.read()
print(f"{len(payload)}:{hashlib.sha256(payload).hexdigest()}", end="")
"""

    result = run_bounded_process(
        [sys.executable, "-c", source],
        input_bytes=payload,
        timeout_seconds=20,
        stdout_maximum_bytes=1024,
        stderr_maximum_bytes=1024,
        require_enforceable=sys.platform == "win32",
    )

    assert len(payload) > 4 * 1024 * 1024
    assert result.stdout == expected


def test_bounded_process_preserves_timeout_while_large_stdin_is_blocked() -> None:
    """A blocked writer cannot mask the execution deadline with broken pipe."""
    payload = b"x" * (5 * 1024 * 1024)

    with pytest.raises(BoundedProcessTimeout):
        run_bounded_process(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            input_bytes=payload,
            timeout_seconds=2,
            stdout_maximum_bytes=1024,
            stderr_maximum_bytes=1024,
            require_enforceable=sys.platform == "win32",
        )


def test_bounded_process_preserves_output_limit_while_large_stdin_is_blocked() -> None:
    """A blocked writer cannot mask a concurrently observed output overflow."""
    payload = b"x" * (5 * 1024 * 1024)
    source = "import sys, time; sys.stdout.write('x' * 10000); sys.stdout.flush(); time.sleep(60)"

    with pytest.raises(BoundedProcessOutputLimit, match="stdout"):
        run_bounded_process(
            [sys.executable, "-c", source],
            input_bytes=payload,
            timeout_seconds=5,
            stdout_maximum_bytes=128,
            stderr_maximum_bytes=128,
            require_enforceable=sys.platform == "win32",
        )


def test_bounded_process_surfaces_real_exit_status_when_child_exits_mid_stdin_write() -> None:
    """clio-relay#209 H1: an early child exit must not mask its own exit status.

    A large stdin payload (the one-pass cold bootstrap script routinely runs
    to tens of megabytes) streams through a writer thread. If the child
    fails fast -- reads only a small prefix and exits with an error -- the
    writer thread's remaining write() hits a broken pipe. Before this fix
    that broken pipe was reported as a generic
    "process stdin could not be written" `BoundedProcessError`, discarding
    the child's real exit code AND its already-drained stderr explaining
    WHY (proven against bootstrap_one_pass_script.py's caller: a mkdir
    collision on the remote end was reported to the operator as an opaque
    transport failure instead of "mkdir: File exists"). The fix: once the
    child has already exited, a stdin write failure is a SYMPTOM, not an
    independent failure -- fall through to the real returncode/stderr.
    """
    payload = b"x" * (32 * 1024 * 1024)
    source = (
        "import sys\n"
        "sys.stdin.buffer.read(64)\n"
        "sys.stderr.write('simulated early remote failure: mkdir: File exists')\n"
        "sys.exit(37)\n"
    )

    result = run_bounded_process(
        [sys.executable, "-c", source],
        input_bytes=payload,
        timeout_seconds=30,
        stdout_maximum_bytes=1024,
        stderr_maximum_bytes=1024,
        require_enforceable=sys.platform == "win32",
    )

    # The exact numeric code is platform-containment-dependent (Windows job-
    # object/broker wrapping can translate it); what matters -- and what was
    # lost before this fix, replaced by a generic BoundedProcessError -- is
    # that SOME real failure status and the child's own stderr both survive.
    assert result.returncode != 0
    assert "simulated early remote failure: mkdir: File exists" in result.stderr


def test_bounded_process_does_not_hide_failed_startup_cleanup(
    monkeypatch: MonkeyPatch,
) -> None:
    """A startup deadline cannot hide containment cleanup failure evidence."""

    def fail_spawn(*_args: object, **_kwargs: object) -> object:
        raise OwnedProcessSpawnError(
            process_id=123,
            mode="test",
            cleanup_errors=["termination failed"],
            cause=RuntimeError("broker readiness timed out"),
        )

    monkeypatch.setattr(bounded_process, "spawn_owned_process", fail_spawn)

    with pytest.raises(BoundedProcessError) as captured:
        run_bounded_process(
            [sys.executable, "-c", "pass"],
            timeout_seconds=0.1,
            stdout_maximum_bytes=128,
            stderr_maximum_bytes=128,
        )

    assert not isinstance(captured.value, BoundedProcessTimeout)
    assert isinstance(captured.value.__cause__, OwnedProcessSpawnError)


def test_bounded_process_release_failure_overrides_primary_timeout(
    monkeypatch: MonkeyPatch,
) -> None:
    """A provider-release failure means cleanup is not verified after termination."""
    real_release = bounded_process.release_owned_process

    def release_then_fail(process: object) -> None:
        real_release(process)  # pyright: ignore[reportArgumentType]
        raise RuntimeError("simulated containment provider release failure")

    monkeypatch.setattr(bounded_process, "release_owned_process", release_then_fail)

    with pytest.raises(BoundedProcessError, match="could not be released") as captured:
        run_bounded_process(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            timeout_seconds=0.5,
            stdout_maximum_bytes=128,
            stderr_maximum_bytes=128,
        )

    assert not isinstance(captured.value, BoundedProcessTimeout)
    assert isinstance(captured.value.__cause__, RuntimeError)

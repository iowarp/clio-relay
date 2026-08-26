"""Live-streaming, bounded subprocess execution for local release checks.

``release_validation.py``'s local release gate ran every check through a
buffered ``subprocess.run(..., capture_output=True)`` with no timeout and no
console output until the process exited: a hanging test produced zero
output for the entire hang (CI's job timeout was the only backstop) and the
evidence report never named what hung (iowarp/clio-relay#275). This module
is the sole owner of the fix: :func:`run_streaming_command` pumps a child
process's stdout/stderr to the caller's console AS EACH LINE ARRIVES while
still capturing the full transcript for evidence, and enforces an explicit
overall deadline that kills the process's WHOLE descendant tree -- never
just the direct child, since an orphaned grandchild still holding the pipe
open would hang the reader threads forever -- and returns a typed timeout
marker instead of a bare nonzero exit.

Kept a standalone leaf module rather than routed through
``clio_relay.process_containment``/``bounded_process.py``: that subsystem is
a much heavier security-isolation mechanism (brokers, systemd cgroup scopes,
Windows job objects, secret-memory gates) built for relay-managed JOB
execution, not a synchronous CLI subprocess pump for ``ruff``/``pytest``/
``uv`` invocations, and it has no per-line echo hook. ``psutil`` is not a
project dependency (``pyproject.toml`` carries none), so tree termination
uses the same platform-native fallback the codebase already applies
elsewhere (``process_containment_windows._terminate_windows_tree``,
``process_containment_posix._signal_posix_tree``): ``taskkill /PID <pid> /T
/F`` on Windows, ``os.killpg`` over a ``start_new_session=True`` process
group on POSIX.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

#: Exit code recorded for a timed-out command -- the same convention GNU
#: coreutils' own ``timeout(1)`` uses, so the sentinel is a recognizable,
#: typed signal rather than an arbitrary magic number.
TIMEOUT_RETURNCODE = 124

#: Bound on how long the pump threads and the killed process get to unwind
#: after a timeout fires, so a leaked descendant that somehow survives the
#: whole-tree kill cannot hang this function forever.
_SHUTDOWN_JOIN_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class StreamedCommandResult:
    """The captured transcript and typed outcome of one streamed command."""

    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool
    timeout_reason: str | None

    def to_completed_process(self) -> subprocess.CompletedProcess[str]:
        """Return an equivalent, evidence-ready ``subprocess.CompletedProcess``.

        A timeout carries no meaningful exit code (the tree was killed out
        from under it), so the returncode is forced to
        :data:`TIMEOUT_RETURNCODE` and the typed reason is appended to
        stderr -- the only channel ``command_evidence``/``ReleaseCommandRunner``
        callers read -- so the recorded failure detail names the timeout
        instead of surfacing as a bare nonzero exit (clio-relay#275's
        no-silent-fallback requirement).
        """
        stderr = self.stderr
        if self.timed_out:
            reason = self.timeout_reason or "check_timeout"
            stderr = f"{stderr}\n{reason}\n" if stderr else f"{reason}\n"
        returncode = TIMEOUT_RETURNCODE if self.timed_out else self.returncode
        return subprocess.CompletedProcess(
            args=self.command,
            returncode=returncode,
            stdout=self.stdout,
            stderr=stderr,
        )


def run_streaming_command(
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: float | None,
    echo: Callable[[str], None] | None = None,
) -> StreamedCommandResult:
    """Run one command, streaming each output line live and capturing it all.

    Args:
        command: The argv to execute.
        cwd: The working directory for the child process.
        timeout_seconds: The overall wall-clock deadline in seconds, or
            ``None`` to wait indefinitely (matches unbounded pre-#275
            behavior for callers that opt out).
        echo: Invoked once per output line, from whichever pump thread reads
            it, for live console visibility. Stdout and stderr each preserve
            their own line order; interleaving between the two streams is
            not guaranteed. ``None`` disables echo (still captures fully).

    Returns:
        The full captured transcript plus a typed timeout marker. Never
        raises on a command timeout -- callers read ``timed_out``.

    Raises:
        ValueError: ``timeout_seconds`` is zero or negative.
        OSError: The child process could not be started.
    """
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("run_streaming_command timeout_seconds must be positive")
    process = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        errors="replace",
        start_new_session=os.name != "nt",
    )
    assert process.stdout is not None
    assert process.stderr is not None

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    echo_lock = threading.Lock()
    stdout_thread = threading.Thread(
        target=_pump,
        args=(process.stdout, stdout_lines, echo, echo_lock),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_pump,
        args=(process.stderr, stderr_lines, echo, echo_lock),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    timed_out = False
    timeout_reason: str | None = None
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        timeout_reason = f"check_timeout after {timeout_seconds:g}s"
        _kill_process_tree(process)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=_SHUTDOWN_JOIN_TIMEOUT_SECONDS)
    stdout_thread.join(timeout=_SHUTDOWN_JOIN_TIMEOUT_SECONDS)
    stderr_thread.join(timeout=_SHUTDOWN_JOIN_TIMEOUT_SECONDS)

    return StreamedCommandResult(
        command=command,
        returncode=-1 if process.returncode is None else process.returncode,
        stdout="\n".join(stdout_lines),
        stderr="\n".join(stderr_lines),
        timed_out=timed_out,
        timeout_reason=timeout_reason,
    )


def _pump(
    stream: TextIO,
    sink: list[str],
    echo: Callable[[str], None] | None,
    echo_lock: threading.Lock,
) -> None:
    """Read one text stream line-by-line, capturing and optionally echoing."""
    try:
        for raw_line in stream:
            line = raw_line.rstrip("\n")
            sink.append(line)
            if echo is not None:
                with echo_lock:
                    echo(line)
    finally:
        stream.close()


def _kill_process_tree(process: subprocess.Popen[str]) -> None:
    """Kill the process and every descendant it may have spawned.

    A bare ``process.kill()`` only signals the immediate child; grandchildren
    (workers pytest-timeout or uv itself forks) would keep the stdout/stderr
    pipes open and hang the reader threads forever, so both platforms get a
    whole-tree kill.
    """
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            check=False,
        )
        return
    with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    with contextlib.suppress(OSError):
        process.kill()

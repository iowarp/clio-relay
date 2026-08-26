"""Live-streaming, bounded subprocess execution primitives (clio-relay#275).

Popen-based process execution that tees a child's stdout/stderr to the
caller's console AS EACH LINE ARRIVES while still capturing the full,
byte-faithful transcript, enforces an explicit overall deadline that kills
the process's WHOLE descendant tree (never just the direct child -- an
orphaned grandchild still holding the pipe open would hang the reader
threads forever), and surfaces every degradation -- a timeout, an echo sink
that raises, a kill that could not be confirmed, a pump that never saw EOF
-- as a typed reason rather than a silent difference in behavior.

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
group on POSIX. POSIX's process-group membership survives the direct
child's own death (a grandchild it spawned stays reachable by group id
regardless), but Windows' ``taskkill /T`` requires its OWN target pid to
still be alive to walk the tree at all -- proven live, this is a genuine
platform limitation with no reliable PID-based workaround short of a Job
Object established at spawn time (the containment subsystem's own
mechanism, deliberately not reused here); :func:`_kill_process_tree` names
that gap with its own ``tree_kill_unreachable`` reason rather than silently
assuming a kill succeeded.

The release-check-specific glue (checkout-path validation, check-id-scoped
echo prefixing, the default injectable runner) lives one layer up, in
``release_check_runtime.py`` -- this module knows nothing about "checks" or
"checkouts", only how to run and observe one process tree.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

#: Exit code recorded for a timed-out command -- the same convention GNU
#: coreutils' own ``timeout(1)`` uses, so the sentinel is a recognizable,
#: typed signal rather than an arbitrary magic number.
TIMEOUT_RETURNCODE = 124

#: Bound on how long BOTH pump threads together get to unwind after the
#: tracked process ends (or is killed), so a leaked descendant that somehow
#: survives a whole-tree kill cannot hang this function forever.
_SHUTDOWN_JOIN_TIMEOUT_SECONDS = 10.0


def format_seconds(value: float) -> str:
    """Format a seconds value without scientific notation (never ``1e+07``)."""
    if value == int(value):
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")


@dataclass(frozen=True)
class StreamedCommandResult:
    """The captured transcript and typed outcome of one streamed command."""

    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool
    timeout_reason: str | None
    degradations: tuple[str, ...] = ()

    def to_completed_process(self) -> subprocess.CompletedProcess[str]:
        """Return an equivalent, evidence-ready ``subprocess.CompletedProcess``.

        A timeout carries no meaningful exit code (the tree was killed out
        from under it), so the returncode is forced to
        :data:`TIMEOUT_RETURNCODE`. Every typed reason -- the timeout itself
        plus any :attr:`degradations` (echo failure, an unconfirmed kill, an
        orphaned descendant) -- is appended to stderr, the only channel
        ``command_evidence``/``ReleaseCommandRunner`` callers read, so none
        of it can surface as a silent, unexplained difference in behavior
        (clio-relay#275's no-silent-fallback requirement).
        """
        stderr = self.stderr
        notes = (*((self.timeout_reason,) if self.timed_out else ()), *self.degradations)
        for note in notes:
            stderr = f"{stderr}\n{note}\n" if stderr else f"{note}\n"
        returncode = TIMEOUT_RETURNCODE if self.timed_out else self.returncode
        return subprocess.CompletedProcess(
            args=self.command,
            returncode=returncode,
            stdout=self.stdout,
            stderr=stderr,
        )


@dataclass
class _StreamCapture:
    """One pipe's accumulated lines plus whether its last line was terminated."""

    lines: list[str] = field(default_factory=list[str])
    ends_with_newline: bool = False


class _EchoGuard:
    """Wrap the caller's echo callback so a failing echo cannot kill a pump.

    A raised exception (``BrokenPipeError`` on a piped/closed console stream
    is realistic) is caught, echo is disabled for the rest of the run
    (repeating a broken sink on every remaining line is noise, not signal),
    and ONE typed reason is recorded so the degradation still reaches the
    result -- capture itself is never interrupted.
    """

    def __init__(self, echo: Callable[[str], None] | None) -> None:
        self._echo = echo
        self._lock = threading.Lock()
        self.failure: str | None = None

    def emit(self, line: str) -> None:
        """Echo one line through the active sink; disable it on failure."""
        with self._lock:
            echo = self._echo
            if echo is None:
                return
            try:
                echo(line)
            except Exception as exc:
                self.failure = f"echo_failed: {type(exc).__name__}: {exc}"
                self._echo = None


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
            not guaranteed. ``None`` disables echo (still captures fully). A
            raising echo is caught and disabled (see :class:`_EchoGuard`);
            it never aborts capture.

    Returns:
        The full, byte-faithful captured transcript plus a typed timeout
        marker and any other typed degradation. Never raises on a command
        timeout or an echo failure -- callers read ``timed_out``/
        ``degradations``.

    Raises:
        ValueError: ``timeout_seconds`` is zero or negative.
        OSError: The child process could not be started.
        BaseException: Whatever interrupted the wait (e.g.
            ``KeyboardInterrupt`` -- the exact interactive scenario this
            module exists to serve), after killing the whole process tree
            and joining the pumps, matching ``subprocess.run``'s own
            cleanup-then-reraise contract.
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

    stdout_capture = _StreamCapture()
    stderr_capture = _StreamCapture()
    echo_guard = _EchoGuard(echo)
    stdout_thread = threading.Thread(
        target=_pump, args=(process.stdout, stdout_capture, echo_guard), daemon=True
    )
    stderr_thread = threading.Thread(
        target=_pump, args=(process.stderr, stderr_capture, echo_guard), daemon=True
    )
    stdout_thread.start()
    stderr_thread.start()

    timed_out = False
    timeout_reason: str | None = None
    degradations: list[str] = []
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        assert timeout_seconds is not None
        timed_out = True
        timeout_reason = f"check_timeout after {format_seconds(timeout_seconds)}s"
        degradations.extend(_kill_process_tree(process))
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=_SHUTDOWN_JOIN_TIMEOUT_SECONDS)
    except BaseException:
        _kill_process_tree(process)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=_SHUTDOWN_JOIN_TIMEOUT_SECONDS)
        _join_pumps(stdout_thread, stderr_thread, deadline_seconds=_SHUTDOWN_JOIN_TIMEOUT_SECONDS)
        raise

    if _join_pumps(stdout_thread, stderr_thread, deadline_seconds=_SHUTDOWN_JOIN_TIMEOUT_SECONDS):
        degradations.append(
            "orphan_descendant: a pump thread did not finish within the shutdown "
            "deadline (a descendant likely still holds the pipe open); transcript "
            "may be truncated"
        )
        degradations.extend(_kill_process_tree(process))
    if echo_guard.failure is not None:
        degradations.append(echo_guard.failure)

    # Snapshot only after every join/kill attempt above -- a pump thread that
    # is still alive past the shutdown deadline could otherwise still be
    # appending while this reads the lists (D8).
    stdout_lines = list(stdout_capture.lines)
    stderr_lines = list(stderr_capture.lines)

    return StreamedCommandResult(
        command=command,
        returncode=-1 if process.returncode is None else process.returncode,
        stdout=_join_transcript(stdout_lines, stdout_capture.ends_with_newline),
        stderr=_join_transcript(stderr_lines, stderr_capture.ends_with_newline),
        timed_out=timed_out,
        timeout_reason=timeout_reason,
        degradations=tuple(degradations),
    )


def _join_transcript(lines: list[str], ends_with_newline: bool) -> str:
    """Rejoin captured lines byte-faithfully to a plain ``.read()`` capture."""
    if not lines:
        return ""
    text = "\n".join(lines)
    return f"{text}\n" if ends_with_newline else text


def _join_pumps(
    stdout_thread: threading.Thread,
    stderr_thread: threading.Thread,
    *,
    deadline_seconds: float,
) -> bool:
    """Join both pump threads against ONE shared deadline (not sequential).

    Returns whether either thread is still alive afterward -- true only when
    a descendant is still holding a pipe open past the shutdown deadline.
    """
    deadline = time.monotonic() + deadline_seconds
    stdout_thread.join(timeout=max(0.0, deadline - time.monotonic()))
    stderr_thread.join(timeout=max(0.0, deadline - time.monotonic()))
    return stdout_thread.is_alive() or stderr_thread.is_alive()


def _pump(stream: TextIO, capture: _StreamCapture, echo_guard: _EchoGuard) -> None:
    """Read one text stream line-by-line, capturing and optionally echoing."""
    try:
        for raw_line in stream:
            capture.ends_with_newline = raw_line.endswith("\n")
            line = raw_line.rstrip("\n")
            capture.lines.append(line)
            echo_guard.emit(line)
    finally:
        stream.close()


def _kill_process_tree(process: subprocess.Popen[str]) -> tuple[str, ...]:
    """Kill the process and every descendant it may have spawned.

    A bare ``process.kill()`` only signals the immediate child; grandchildren
    (workers pytest-timeout or uv itself forks) would keep the stdout/stderr
    pipes open and hang the reader threads forever, so both platforms get a
    whole-tree kill. Returns zero or more typed reasons when a kill step
    could not be CONFIRMED to have succeeded -- a failed kill must never be
    indistinguishable from a clean one.
    """
    if os.name == "nt":
        result = subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode == 0:
            return ()
        detail = (result.stderr or result.stdout or "").strip()
        if result.returncode == 128:
            # taskkill validates that its OWN target pid is still alive
            # BEFORE it walks the tree at all -- proven live (a repro
            # spawning a grandchild, letting the direct child exit, then
            # querying Win32_Process for children of the now-dead pid):
            # once the parent has exited, /T is a no-op, and -- contrary to
            # the "Windows remembers the creator pid forever" assumption --
            # a live child's ParentProcessId stops being reliably queryable
            # via WMI too, the instant the parent process object is gone.
            # There is no PID-based way left to reach a surviving
            # descendant from here without a Job Object established at
            # spawn time (the mechanism clio_relay.process_containment's
            # heavier subsystem uses -- deliberately not reused, see this
            # module's own docstring). Surfaced explicitly as a distinct
            # typed reason rather than folded into "clean" or "uncertain".
            return (
                "tree_kill_unreachable: taskkill exit=128 (the tracked process "
                "already exited before the kill was attempted; any descendant "
                "it spawned is not confirmed killed -- Windows offers no "
                f"reliable PID-based way to reach it from here): {detail}",
            )
        return (f"tree_kill_uncertain: taskkill exit={result.returncode}: {detail}",)
    reasons: list[str] = []
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass  # already gone -- not a failure
    except (PermissionError, OSError) as exc:
        reasons.append(f"tree_kill_uncertain: killpg failed: {exc}")
    with contextlib.suppress(OSError):
        process.kill()
    return tuple(reasons)

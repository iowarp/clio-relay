"""One remote call's output, its end, and the watch over it while it runs.

A call ends when its script exits on the site, not when the connection lets
go: the script reports its exit code on stderr behind a marker
(``ends_with_marker``). The same watch (``supervise``) runs a call over relay's
held connection (``held``) and over the system ``ssh`` (``run_process``).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import secrets
import signal
import subprocess
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from clio_relay import programs
from clio_relay.adapters.ssh import overlapped_stdin
from clio_relay.jobs.handles import STALL_S, ExecResult, StallReport

logger = logging.getLogger(__name__)

POSIX = os.name == "posix"
EXIT_GRACE_S = 5.0
"""After the remote command exits, how long its channel gets to end by itself."""


@dataclass
class Completed:
    """One finished call, with its output as bytes."""

    argv: list[str]
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    timed_out: bool = False
    duration_s: float = 0.0
    remote_exit: bool = False
    """The remote command's exit was seen (its marker), so it ran to its end."""

    def text(self) -> str:
        """Return stdout as text."""
        return self.stdout.decode("utf-8", errors="replace")

    def error_text(self) -> str:
        """Return stderr as text."""
        return self.stderr.decode("utf-8", errors="replace")

    def to_exec_result(self) -> ExecResult:
        """Return the result in the ``exec`` verb's shape."""
        return ExecResult(
            argv=self.argv,
            exit_code=self.exit_code,
            stdout=self.text(),
            stderr=self.error_text(),
            stdout_truncated=self.stdout_truncated,
            stderr_truncated=self.stderr_truncated,
            timed_out=self.timed_out,
            duration_s=round(self.duration_s, 3),
        )


class Reader(Protocol):
    """A byte stream: an ``asyncio.StreamReader`` or an ssh channel's reader."""

    async def read(self, n: int = -1, /) -> Any: ...


class Sink:
    """One output stream, held to ``limit`` bytes, that may carry relay's exit marker.

    With a ``marker``, the stream's last bytes are held back until it is clear they
    are not the marker, so the marker never reaches ``data``. Seeing it records the
    remote exit code and sets ``exited``.
    """

    def __init__(self, limit: int | None, marker: bytes | None = None) -> None:
        self.limit = limit
        self.data = bytearray()
        self.truncated = False
        self.last = time.monotonic()
        self.marker = marker
        self.exit_code: int | None = None
        self.exited = asyncio.Event()
        self._held = b""
        self._pattern = (
            re.compile(rb"(?:\r?\n)?" + re.escape(marker) + rb"(\d{1,3})\r?\n") if marker else None
        )

    async def pump(self, stream: Reader) -> None:
        try:
            while chunk := await stream.read(65536):
                self.last = time.monotonic()
                self._take(bytes(chunk))
        finally:
            self._keep(self._held)
            self._held = b""

    def _take(self, chunk: bytes) -> None:
        if self.marker is None or self._pattern is None or self.exited.is_set():
            self._keep(chunk)
            return
        buffer = self._held + chunk
        found = self._pattern.search(buffer)
        if found is not None:
            self._keep(buffer[: found.start()])
            self._keep(buffer[found.end() :])
            self._held = b""
            self.exit_code = int(found.group(1))
            self.exited.set()
            return
        hold = len(self.marker) + 8
        self._keep(buffer[:-hold])
        self._held = buffer[-hold:]

    def _keep(self, chunk: bytes) -> None:
        if not chunk:
            return
        if self.limit is None:
            self.data += chunk
            return
        room = self.limit - len(self.data)
        if room > 0:
            self.data += chunk[:room]
        if len(chunk) > room:
            self.truncated = True


def exit_marker() -> bytes:
    """Return a marker no command's output holds: a fresh random token per call."""
    return f"clio-relay-exit:{secrets.token_hex(16)}:".encode()


def ends_with_marker(script: str, marker: bytes) -> str:
    """Return ``script`` run in a subshell, then the marker and its exit code on stderr.

    The marker says the script ended, whatever still holds the session's output or
    keeps the channel up afterwards: a background child, a jump host's proxy, a wrapper.
    """
    return (
        f"(\n{script.rstrip(chr(10))}\n)\n"
        "rc=$?\n"
        f"printf '\\n%s%d\\n' '{marker.decode()}' \"$rc\" >&2\n"
        'exit "$rc"\n'
    )


def signed(code: int | None) -> int | None:
    """Return an exit code as POSIX reads it; Windows reports -1 as 4294967295."""
    return code - 2**32 if code is not None and code >= 2**31 else code


async def supervise(
    work: asyncio.Future[Any],
    out: Sink,
    err: Sink,
    *,
    started: float,
    timeout_s: float | None,
    on_stall: StallReport | None,
    stall_s: float,
    stop: Callable[[], None],
    name: str,
) -> bool:
    """Wait for ``work`` (the pumps, the feed and the end), and return whether it timed out.

    While both streams are quiet for ``stall_s``, ``on_stall`` is called, then
    again every ``stall_s`` while they stay quiet. Once ``err`` shows the exit
    marker, ``work`` gets ``EXIT_GRACE_S`` and is then ``stop``-ped. Cancelling
    the caller stops it too.
    """
    exited = asyncio.ensure_future(err.exited.wait())
    timed_out = False
    deadline = None if timeout_s is None else started + timeout_s
    report_at = stall_s
    try:
        while True:
            now = time.monotonic()
            idle = now - max(out.last, err.last)
            if idle < stall_s:
                report_at = stall_s
            wait: float | None = None if on_stall is None else max(report_at - idle, 0.05)
            if deadline is not None:
                left = max(deadline - now, 0.0)
                wait = left if wait is None else min(wait, left)
            done, _ = await asyncio.wait(
                {work, exited}, timeout=wait, return_when=asyncio.FIRST_COMPLETED
            )
            if work in done:
                break
            if exited in done:
                # The remote command has exited; whatever holds the call up now is not it.
                done, _ = await asyncio.wait({work}, timeout=EXIT_GRACE_S)
                if not done:
                    logger.info(
                        "%s: the remote command exited, but it stayed up; stopping it", name
                    )
                    stop()
                    done, _ = await asyncio.wait({work}, timeout=10)
                    if not done:
                        work.cancel()
                break
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                timed_out = True
                stop()
                done, _ = await asyncio.wait({work}, timeout=10)
                if not done:
                    work.cancel()
                break
            idle = now - max(out.last, err.last)
            if on_stall is not None and idle >= report_at:
                report_at += stall_s
                await on_stall(idle, report_at - idle)
        with contextlib.suppress(asyncio.CancelledError):
            await work
    except asyncio.CancelledError:
        stop()
        work.cancel()
        raise
    finally:
        exited.cancel()
    return timed_out


def _kill(process: asyncio.subprocess.Process) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        if POSIX:
            os.killpg(process.pid, signal.SIGKILL)
            return
        # The whole tree: a wrapper's ssh and ssh's ProxyCommand outlive the wrapper.
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                timeout=10,
                check=False,
            )
        process.kill()


async def run_process(
    argv: Sequence[str],
    *,
    timeout_s: float | None,
    max_output_bytes: int | None = None,
    stdin: bytes | None = None,
    env: Mapping[str, str] | None = None,
    hold_stdin: bool = False,
    on_stall: StallReport | None = None,
    stall_s: float = STALL_S,
    marker: bytes | None = None,
) -> Completed:
    """Run ``argv`` with no shell. ``timeout_s=None`` waits as long as it takes.

    With ``hold_stdin``, standard input stays open until the process ends, so the
    other end sees it close only then. Stalls, the marker and cancelling are as
    ``supervise`` says; stopping kills the process group.
    """
    command = list(argv)
    started = time.monotonic()
    out = Sink(max_output_bytes)
    err = Sink(max_output_bytes, marker)
    piped = stdin is not None or hold_stdin
    program = programs.resolved(command, env)
    if program is None:
        return Completed(command, 127, b"", f"{command[0]}: command not found".encode())
    # Win32-OpenSSH gets an overlapped stdin, or a held one can hang it at the end.
    overlapped = await overlapped_stdin.open_for(program[0], env) if piped else None
    child_stdin = asyncio.subprocess.PIPE if piped else asyncio.subprocess.DEVNULL
    if overlapped is not None and overlapped.child is not None:
        child_stdin = overlapped.child
    try:
        process = await asyncio.create_subprocess_exec(
            *program,
            stdin=child_stdin,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=overlapped.env if overlapped else dict(env) if env is not None else None,
            start_new_session=POSIX,
        )
    except BaseException as exc:
        if overlapped is not None:
            overlapped.close()
        if isinstance(exc, FileNotFoundError):
            return Completed(command, 127, b"", f"{command[0]}: command not found".encode())
        raise
    if overlapped is not None:
        overlapped.spawned()
    sink = overlapped.writer if overlapped is not None else process.stdin
    assert process.stdout is not None and process.stderr is not None

    async def _feed() -> None:
        if sink is None:
            return
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            if stdin:
                sink.write(stdin)
                await sink.drain()
        if hold_stdin:
            return
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            sink.close()

    parts: list[Awaitable[Any]] = [
        out.pump(process.stdout),
        err.pump(process.stderr),
        _feed(),
        process.wait(),
    ]
    work = asyncio.ensure_future(asyncio.gather(*parts))
    try:
        timed_out = await supervise(
            work,
            out,
            err,
            started=started,
            timeout_s=timeout_s,
            on_stall=on_stall,
            stall_s=stall_s,
            stop=lambda: _kill(process),
            name=command[0],
        )
    finally:
        if sink is not None:
            with contextlib.suppress(BrokenPipeError, ConnectionResetError, RuntimeError):
                sink.close()
        transport = getattr(process, "_transport", None)
        if transport is not None:
            transport.close()
    remote = err.exited.is_set()
    return Completed(
        argv=command,
        exit_code=err.exit_code if remote else None if timed_out else signed(process.returncode),
        stdout=bytes(out.data),
        stderr=bytes(err.data),
        stdout_truncated=out.truncated,
        stderr_truncated=err.truncated,
        timed_out=timed_out and not remote,
        duration_s=time.monotonic() - started,
        remote_exit=remote,
    )

"""Bounded subprocess execution, shared by adapters that run local commands."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import time
from collections.abc import Mapping
from pathlib import Path

from clio_relay.jobs.handles import STALL_S, ExecResult, StallReport

POSIX = os.name == "posix"
MAX_STALL_STEP_S = 600.0


class _Sink:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.data = bytearray()
        self.truncated = False
        self.last = time.monotonic()

    async def pump(self, stream: asyncio.StreamReader) -> None:
        while chunk := await stream.read(65536):
            self.last = time.monotonic()
            room = self.limit - len(self.data)
            if room > 0:
                self.data += chunk[:room]
            if len(chunk) > room:
                self.truncated = True

    def text(self) -> str:
        return self.data.decode("utf-8", errors="replace")


def _kill(process: asyncio.subprocess.Process) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        if POSIX:
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()


async def run_bounded(
    argv: list[str],
    *,
    timeout_s: float | None = None,
    max_output_bytes: int,
    cwd: Path | str | None = None,
    env: Mapping[str, str] | None = None,
    stdin: bytes | None = None,
    on_stall: StallReport | None = None,
    stall_s: float = STALL_S,
) -> ExecResult:
    """Run ``argv`` with no shell, keep at most ``max_output_bytes`` of each stream.

    With ``timeout_s`` the whole process group is killed at that limit and the
    output read so far is returned with ``timed_out`` set. Without it the command
    runs until it ends; after ``stall_s`` with no output, ``on_stall`` is called,
    then again after waits that double (capped at ``MAX_STALL_STEP_S``) while it
    stays quiet. Cancelling the caller kills the process group. A missing
    executable is exit code 127.
    """
    started = time.monotonic()
    stdout = _Sink(max_output_bytes)
    stderr = _Sink(max_output_bytes)
    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            env=dict(env) if env is not None else None,
            start_new_session=POSIX,
        )
    except FileNotFoundError:
        return ExecResult(
            argv=argv,
            exit_code=127,
            stdout="",
            stderr=f"{argv[0]}: command not found",
            duration_s=time.monotonic() - started,
        )
    except PermissionError as exc:
        return ExecResult(
            argv=argv,
            exit_code=126,
            stdout="",
            stderr=f"{argv[0]}: {exc.strerror or 'permission denied'}",
            duration_s=time.monotonic() - started,
        )
    assert process.stdout is not None and process.stderr is not None

    async def _feed() -> None:
        if stdin is None or process.stdin is None:
            return
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            process.stdin.write(stdin)
            await process.stdin.drain()
            process.stdin.close()

    timed_out = False
    work = [
        asyncio.ensure_future(step)
        for step in (
            stdout.pump(process.stdout),
            stderr.pump(process.stderr),
            _feed(),
            process.wait(),
        )
    ]
    deadline = None if timeout_s is None else started + timeout_s
    report_at = stall_s
    try:
        while True:
            now = time.monotonic()
            idle = now - max(stdout.last, stderr.last)
            if idle < stall_s:
                report_at = stall_s
            wait = max(report_at - idle, 0.05)
            if deadline is not None:
                wait = min(wait, max(deadline - now, 0.0))
            _, pending = await asyncio.wait(work, timeout=wait)
            if not pending:
                break
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                timed_out = True
                _kill(process)
                # Read the pipes to their end so their transports close on this loop.
                await asyncio.wait(work, timeout=5)
                break
            idle = now - max(stdout.last, stderr.last)
            if idle >= report_at:
                report_at += min(report_at, MAX_STALL_STEP_S)
                if on_stall is not None:
                    await on_stall(idle, report_at - idle)
        for step in work:
            if step.done() and not step.cancelled() and (error := step.exception()) is not None:
                raise error
    except BaseException:
        if process.returncode is None:
            _kill(process)
        raise
    finally:
        for step in work:
            step.cancel()
        await asyncio.gather(*work, return_exceptions=True)
    return ExecResult(
        argv=argv,
        exit_code=None if timed_out else process.returncode,
        stdout=stdout.text(),
        stderr=stderr.text(),
        stdout_truncated=stdout.truncated,
        stderr_truncated=stderr.truncated,
        timed_out=timed_out,
        duration_s=round(time.monotonic() - started, 3),
    )

"""Bounded subprocess execution, shared by adapters that run local commands."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import time
from collections.abc import Mapping
from pathlib import Path

from clio_relay.jobs.handles import ExecResult

POSIX = os.name == "posix"


class _Sink:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.data = bytearray()
        self.truncated = False

    async def pump(self, stream: asyncio.StreamReader) -> None:
        while chunk := await stream.read(65536):
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
    timeout_s: float,
    max_output_bytes: int,
    cwd: Path | str | None = None,
    env: Mapping[str, str] | None = None,
    stdin: bytes | None = None,
) -> ExecResult:
    """Run ``argv`` with no shell, keep at most ``max_output_bytes`` of each stream.

    On timeout the whole process group is killed and the output read so far is
    returned with ``timed_out`` set. A missing executable is exit code 127.
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
    try:
        _, pending = await asyncio.wait(work, timeout=timeout_s)
        if pending:
            timed_out = True
            _kill(process)
            # Read the pipes to their end so their transports close on this loop.
            await asyncio.wait(work, timeout=5)
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

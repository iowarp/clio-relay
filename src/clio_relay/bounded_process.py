"""Bounded subprocess execution with relay-owned descendant containment."""

from __future__ import annotations

import subprocess
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, cast

from clio_relay.process_containment import (
    OwnedProcessSpawnError,
    release_owned_process,
    spawn_owned_process,
    terminate_owned_process,
)


class BoundedProcessError(RuntimeError):
    """Base error for a bounded process that could not be safely observed."""


class BoundedProcessTimeout(BoundedProcessError):
    """The process tree exceeded its finite execution deadline."""


class BoundedProcessOutputLimit(BoundedProcessError):
    """The process tree exceeded an explicit stdout or stderr byte bound."""


class BoundedProcessTreeLeak(BoundedProcessError):
    """The direct child exited while a descendant remained in its containment."""


@dataclass(frozen=True, slots=True)
class _CapturedStream:
    value: bytearray
    overflow: threading.Event
    errors: list[OSError | ValueError]


def run_bounded_process(
    command: list[str],
    *,
    cwd: Path | None = None,
    environment: Mapping[str, str] | None = None,
    input_bytes: bytes | None = None,
    timeout_seconds: float,
    stdout_maximum_bytes: int,
    stderr_maximum_bytes: int,
    require_enforceable: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run one finite process tree while retaining only bounded output bytes."""
    if not command or any(not value for value in command):
        raise ValueError("bounded process command must contain non-empty arguments")
    if timeout_seconds <= 0:
        raise ValueError("bounded process timeout must be positive")
    if stdout_maximum_bytes < 1 or stderr_maximum_bytes < 1:
        raise ValueError("bounded process output limits must be positive")
    deadline = time.monotonic() + timeout_seconds
    stdout = _CapturedStream(bytearray(), threading.Event(), [])
    stderr = _CapturedStream(bytearray(), threading.Event(), [])
    try:
        process = cast(
            subprocess.Popen[bytes],
            spawn_owned_process(
                command,
                cwd=cwd,
                env=(dict(environment) if environment is not None else None),
                interactive_stdin=input_bytes is not None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
                startup_timeout_seconds=min(10.0, max(0.001, deadline - time.monotonic())),
                require_enforceable=require_enforceable,
            ),
        )
    except OSError:
        raise
    except Exception as exc:
        cleanup_failed = isinstance(exc, OwnedProcessSpawnError) and not exc.cleanup_verified
        if not cleanup_failed and (
            time.monotonic() >= deadline
            or isinstance(exc, subprocess.TimeoutExpired)
            or "timed out"
            in (
                exc.startup_error_message if isinstance(exc, OwnedProcessSpawnError) else str(exc)
            ).lower()
        ):
            raise BoundedProcessTimeout(
                f"process tree exceeded {timeout_seconds:g} seconds during containment startup: "
                f"{command[0]}"
            ) from exc
        raise BoundedProcessError(
            f"process containment could not be established: {command[0]}"
        ) from exc
    assert process.stdout is not None
    assert process.stderr is not None

    def drain(
        stream: BinaryIO,
        captured: _CapturedStream,
        *,
        maximum: int,
        keep_tail: bool,
    ) -> None:
        try:
            while chunk := stream.read(8192):
                if len(captured.value) + len(chunk) > maximum:
                    captured.overflow.set()
                if keep_tail:
                    captured.value.extend(chunk)
                    if len(captured.value) > maximum:
                        del captured.value[: len(captured.value) - maximum]
                else:
                    available = max(0, maximum - len(captured.value))
                    captured.value.extend(chunk[:available])
        except (OSError, ValueError) as exc:
            captured.errors.append(exc)

    stdout_thread = threading.Thread(
        target=drain,
        args=(process.stdout, stdout),
        kwargs={"maximum": stdout_maximum_bytes, "keep_tail": False},
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=drain,
        args=(process.stderr, stderr),
        kwargs={"maximum": stderr_maximum_bytes, "keep_tail": True},
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()
    stdin_errors: list[OSError | ValueError] = []
    stdin_thread: threading.Thread | None = None
    failure: BoundedProcessError | None = None
    if input_bytes is not None:
        if process.stdin is None:
            failure = BoundedProcessError(f"process stdin was unavailable: {command[0]}")

        else:

            def write_stdin() -> None:
                stream = process.stdin
                if stream is None:  # pragma: no cover - guarded before thread start
                    stdin_errors.append(ValueError("process stdin disappeared"))
                    return
                try:
                    view = memoryview(input_bytes)
                    while view:
                        written = stream.write(view)
                        if written <= 0:
                            raise OSError("process stdin write made no progress")
                        view = view[written:]
                    stream.flush()
                except (OSError, ValueError) as exc:
                    stdin_errors.append(exc)
                finally:
                    try:
                        stream.close()
                    except (OSError, ValueError) as exc:
                        stdin_errors.append(exc)

            stdin_thread = threading.Thread(
                target=write_stdin,
                name=f"clio-relay-bounded-stdin-{process.pid}",
                daemon=True,
            )
            stdin_thread.start()
    try:
        while failure is None and process.poll() is None:
            if stdout.overflow.is_set() or stderr.overflow.is_set():
                stream_name = "stdout" if stdout.overflow.is_set() else "stderr"
                failure = BoundedProcessOutputLimit(
                    f"process {stream_name} exceeded its byte bound: {command[0]}"
                )
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                failure = BoundedProcessTimeout(
                    f"process tree exceeded {timeout_seconds:g} seconds: {command[0]}"
                )
                break
            time.sleep(min(0.01, remaining))
        if failure is None and stdin_thread is not None:
            stdin_thread.join(timeout=max(0.0, deadline - time.monotonic()))
            if stdin_thread.is_alive():
                failure = BoundedProcessTimeout(
                    f"process stdin exceeded {timeout_seconds:g} seconds: {command[0]}"
                )
            elif stdin_errors:
                failure = BoundedProcessError(f"process stdin could not be written: {command[0]}")
                failure.__cause__ = stdin_errors[0]
        if failure is not None:
            try:
                terminate_owned_process(cast(subprocess.Popen[str], process))
            except Exception as exc:
                failure = BoundedProcessError(
                    f"process containment termination failed: {command[0]}"
                )
                failure.__cause__ = exc
        else:
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            try:
                release_owned_process(cast(subprocess.Popen[str], process))
            except RuntimeError as exc:
                try:
                    terminate_owned_process(cast(subprocess.Popen[str], process))
                except Exception as cleanup_exc:
                    failure = BoundedProcessError(
                        f"process containment termination failed: {command[0]}"
                    )
                    failure.__cause__ = cleanup_exc
                else:
                    failure = BoundedProcessTreeLeak(
                        f"process left a descendant after direct exit: {command[0]}"
                    )
                    failure.__cause__ = exc
    except subprocess.TimeoutExpired as exc:
        try:
            terminate_owned_process(cast(subprocess.Popen[str], process))
        except Exception as cleanup_exc:
            failure = BoundedProcessError(f"process containment termination failed: {command[0]}")
            failure.__cause__ = cleanup_exc
        else:
            failure = BoundedProcessTimeout(
                f"process tree exceeded {timeout_seconds:g} seconds: {command[0]}"
            )
            failure.__cause__ = exc
    finally:
        try:
            release_owned_process(cast(subprocess.Popen[str], process))
        except RuntimeError as exc:
            failure = BoundedProcessError(
                f"process containment could not be released: {command[0]}"
            )
            failure.__cause__ = exc
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        if stdin_thread is not None:
            stdin_thread.join(timeout=5)
    if (
        stdout_thread.is_alive()
        or stderr_thread.is_alive()
        or (stdin_thread is not None and stdin_thread.is_alive())
    ):
        raise BoundedProcessTreeLeak(f"process I/O collectors did not terminate: {command[0]}")
    if failure is not None:
        raise failure
    if stdout.overflow.is_set() or stderr.overflow.is_set():
        stream_name = "stdout" if stdout.overflow.is_set() else "stderr"
        raise BoundedProcessOutputLimit(
            f"process {stream_name} exceeded its byte bound: {command[0]}"
        )
    if stdout.errors or stderr.errors:
        raise BoundedProcessError(f"process output could not be read: {command[0]}")
    if stdin_errors:
        error = BoundedProcessError(f"process stdin could not be written: {command[0]}")
        error.__cause__ = stdin_errors[0]
        raise error
    return subprocess.CompletedProcess(
        args=command,
        returncode=process.returncode,
        stdout=bytes(stdout.value).decode("utf-8", errors="replace"),
        stderr=bytes(stderr.value).decode("utf-8", errors="replace"),
    )

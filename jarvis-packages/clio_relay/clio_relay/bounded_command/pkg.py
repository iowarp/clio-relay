"""JARVIS-CD package for bounded relay commands."""

from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO, cast

from clio_relay._jarvis_api import Application
from clio_relay.bounded_command.progress import adapter_from_config, append_progress_record
from clio_relay.process_containment import nested_popen_kwargs, terminate_nested_process

PROGRESS_FILE_ENV = "CLIO_RELAY_PROGRESS_FILE"
PROGRESS_TOKEN_ENV = "CLIO_RELAY_PROGRESS_TOKEN"
RUNTIME_FILE_ENV = "CLIO_RELAY_RUNTIME_METADATA_FILE"
OUTPUT_READ_MAX_CHARACTERS = 65_536
OUTPUT_QUEUE_MAX_CHUNKS = 64
OUTPUT_TAIL_MAX_CHARACTERS = 1_048_576


@dataclass
class _BoundedTextTail:
    """Retain a bounded tail while the complete stream is forwarded live."""

    limit: int = OUTPUT_TAIL_MAX_CHARACTERS
    chunks: deque[str] = field(default_factory=lambda: deque[str]())
    size: int = 0

    def append(self, value: str) -> None:
        """Append text, discarding the oldest characters above the limit."""
        if not value or self.limit <= 0:
            return
        if len(value) >= self.limit:
            self.chunks.clear()
            self.chunks.append(value[-self.limit :])
            self.size = self.limit
            return
        self.chunks.append(value)
        self.size += len(value)
        while self.size > self.limit:
            excess = self.size - self.limit
            oldest = self.chunks[0]
            if len(oldest) <= excess:
                self.chunks.popleft()
                self.size -= len(oldest)
                continue
            self.chunks[0] = oldest[excess:]
            self.size -= excess

    def render(self) -> str:
        """Return the retained stream tail."""
        return "".join(self.chunks)


class BoundedCommand(Application):
    """Execute a bounded command and let JARVIS-CD capture provenance."""

    def _init(self) -> None:
        """Initialize package state."""

    def _configure_menu(self) -> list[dict[str, Any]]:
        """Return JARVIS configurator options."""
        return []

    def _configure(self, **kwargs: Any) -> None:
        """Store configuration provided by the pipeline YAML."""
        self.config.update(kwargs)

    def start(self) -> None:
        """Run the configured command."""
        command = self.config.get("command")
        if not isinstance(command, list):
            raise ValueError("command must be a string array")
        raw_command = cast(list[object], command)
        if not all(isinstance(item, str) for item in raw_command):
            raise ValueError("command must be a string array")
        command_args = [cast(str, item) for item in raw_command]
        env = os.environ.copy()
        supplied_env = self.config.get("env", {})
        if isinstance(supplied_env, dict):
            typed_env = cast(dict[object, object], supplied_env)
            env.update({str(key): str(value) for key, value in typed_env.items()})
        env = _scrub_relay_environment(env)
        workdir_value = self.config.get("workdir")
        workdir = Path(workdir_value) if isinstance(workdir_value, str) else None
        timeout_value = self.config.get("timeout_seconds")
        timeout = int(timeout_value) if timeout_value is not None else None
        result = _run_streaming(
            command_args,
            cwd=workdir,
            env=env,
            timeout=timeout,
            progress_config=self.config.get("progress"),
        )
        if result.returncode != 0:
            raise RuntimeError(f"command failed with exit code {result.returncode}")

    def stop(self) -> None:
        """Stop hook for bounded commands."""

    def clean(self) -> None:
        """Clean hook for bounded commands."""


def _run_streaming(
    command: list[str],
    *,
    cwd: Path | None,
    env: dict[str, str],
    timeout: int | None,
    progress_config: object,
) -> subprocess.CompletedProcess[str]:
    adapter = adapter_from_config(progress_config)
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        **nested_popen_kwargs(env),
    )
    stdout_tail = _BoundedTextTail()
    stderr_tail = _BoundedTextTail()
    output_queue: queue.Queue[tuple[str, str | None]] = queue.Queue(maxsize=OUTPUT_QUEUE_MAX_CHUNKS)

    def read_stream(name: str, stream: TextIO) -> None:
        try:
            while chunk := stream.readline(OUTPUT_READ_MAX_CHARACTERS):
                output_queue.put((name, chunk))
        finally:
            output_queue.put((name, None))

    assert process.stdout is not None
    assert process.stderr is not None
    threads = [
        threading.Thread(target=read_stream, args=("stdout", process.stdout), daemon=True),
        threading.Thread(target=read_stream, args=("stderr", process.stderr), daemon=True),
    ]
    for thread in threads:
        thread.start()
    deadline = None if timeout is None else time.monotonic() + timeout
    closed_streams: set[str] = set()

    def retain_and_forward(stream_name: str, line: str) -> None:
        if stream_name == "stdout":
            stdout_tail.append(line)
            print(line, end="", flush=True)
            if adapter is not None:
                for record in adapter.observe_stdout(line):
                    append_progress_record(record)
            return
        stderr_tail.append(line)
        print(line, end="", file=sys.stderr, flush=True)

    try:
        while len(closed_streams) < 2:
            if deadline is not None and time.monotonic() >= deadline:
                assert timeout is not None
                raise subprocess.TimeoutExpired(command, timeout)
            try:
                stream_name, line = output_queue.get(timeout=0.1)
            except queue.Empty:
                if process.poll() is not None and all(not thread.is_alive() for thread in threads):
                    break
                continue
            if line is None:
                closed_streams.add(stream_name)
                continue
            retain_and_forward(stream_name, line)
        returncode = process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        _terminate_process_tree(process)
        _drain_reader_queue(output_queue, threads, stdout_tail, stderr_tail)
        raise
    except Exception:
        _terminate_process_tree(process)
        _drain_reader_queue(output_queue, threads, stdout_tail, stderr_tail)
        raise
    return subprocess.CompletedProcess(
        command,
        returncode,
        stdout=stdout_tail.render(),
        stderr=stderr_tail.render(),
    )


def _terminate_process_tree(process: subprocess.Popen[str]) -> None:
    terminate_nested_process(process)


def _drain_reader_queue(
    output_queue: queue.Queue[tuple[str, str | None]],
    threads: list[threading.Thread],
    stdout_tail: _BoundedTextTail,
    stderr_tail: _BoundedTextTail,
) -> None:
    """Drain readers after termination without racing ``communicate`` on pipes."""
    deadline = time.monotonic() + 15
    while any(thread.is_alive() for thread in threads) or not output_queue.empty():
        if time.monotonic() >= deadline:
            raise RuntimeError("command output readers did not terminate")
        try:
            stream_name, line = output_queue.get(timeout=0.1)
        except queue.Empty:
            continue
        if line is None:
            continue
        if stream_name == "stdout":
            stdout_tail.append(line)
        else:
            stderr_tail.append(line)
    for thread in threads:
        thread.join(timeout=0)


def _scrub_relay_environment(env: dict[str, str]) -> dict[str, str]:
    """Remove relay-owned capabilities before launching application code."""
    for name in list(env):
        if _relay_owned_environment_name(name):
            env.pop(name, None)
    return env


def _relay_owned_environment_name(name: str) -> bool:
    if name in {PROGRESS_FILE_ENV, RUNTIME_FILE_ENV}:
        return True
    return name.startswith("CLIO_RELAY_") and (name.endswith("_TOKEN") or name.endswith("_SECRET"))

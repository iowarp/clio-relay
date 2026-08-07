"""Run a bounded command on the cluster, optionally from a staged local script.

The command is an explicit argument vector executed with no shell interposed,
under a wall-clock limit, with optional structured progress extraction from
stdout. Its script setting declares a local-file input binding: the relay
stages one file from the caller's own machine, ingesting it as an immutable
input artifact and appending the staged cluster path to the command as its
final argument, so a shell script or any other interpreted file written on the
caller's machine can be executed here without being copied by hand.
"""

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

from clio_relay._jarvis_api import Application, ConfigurationInputBinding
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
        """Return the JARVIS configurator options this package accepts.

        JARVIS validates every configuration key against this menu before a
        package is added to a pipeline, so an undeclared setting is rejected
        even though the package would consume it.  Each option that may be
        omitted declares a concrete default: JARVIS treats a menu entry whose
        default is ``None`` as a required parameter.
        """
        return [
            {
                "name": "command",
                "msg": (
                    "Argument vector to execute, starting with the program. No shell is "
                    "interposed, so shell syntax needs an explicit interpreter entry."
                ),
                "type": list,
            },
            {
                "name": "workdir",
                "msg": (
                    "Working directory for the command. An empty string runs it in the "
                    "directory JARVIS selected for the package."
                ),
                "type": str,
                "default": "",
            },
            {
                "name": "env",
                "msg": (
                    "Environment variables added to the inherited environment. Relay-owned "
                    "capability variables are always removed before the command starts."
                ),
                "type": dict,
                "default": {},
            },
            {
                "name": "timeout_seconds",
                "msg": (
                    "Wall-clock limit in seconds after which the command tree is terminated. "
                    "Zero means no limit."
                ),
                "type": int,
                "default": 0,
            },
            {
                "name": "progress",
                "msg": (
                    "Structured progress extraction from stdout: {'adapter': 'regex', "
                    "'pattern': ...} publishes progress records, {'adapter': 'none'} "
                    "publishes none."
                ),
                "type": dict,
                "default": {"adapter": "none"},
            },
            {
                "name": "script",
                "msg": (
                    "Caller-local script staged onto the cluster and appended to 'command' "
                    "as its final argument. The relay snapshots this file, ingests it as an "
                    "immutable input artifact, and rewrites this setting to the staged "
                    "cluster path before JARVIS records the step."
                ),
                "type": str,
                "default": "",
                "input_binding": ConfigurationInputBinding(
                    kind="local_file",
                    structure="regular_file",
                ).to_dict(),
            },
        ]

    def _configure(self, **kwargs: Any) -> None:
        """Store configuration provided by the pipeline YAML or the configurator."""
        if "command" in kwargs:
            _command_arguments(kwargs["command"])
        self.config.update(kwargs)

    def start(self) -> None:
        """Run the configured command."""
        command_args = _command_arguments(self.config.get("command"))
        staged_script = _optional_script(self.config.get("script"))
        if staged_script is not None:
            command_args = [*command_args, staged_script]
        env = os.environ.copy()
        supplied_env = self.config.get("env", {})
        if isinstance(supplied_env, dict):
            typed_env = cast(dict[object, object], supplied_env)
            env.update({str(key): str(value) for key, value in typed_env.items()})
        env = _scrub_relay_environment(env)
        workdir_value = self.config.get("workdir")
        workdir = Path(workdir_value) if isinstance(workdir_value, str) and workdir_value else None
        timeout = _optional_timeout(self.config.get("timeout_seconds"))
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


def _command_arguments(value: object) -> list[str]:
    """Return the configured command as an argument vector."""
    if not isinstance(value, list):
        raise ValueError("command must be a string array")
    raw_command = cast(list[object], value)
    if not raw_command:
        raise ValueError("command must be a non-empty string array")
    if not all(isinstance(item, str) for item in raw_command):
        raise ValueError("command must be a string array")
    return [cast(str, item) for item in raw_command]


def _optional_script(value: object) -> str | None:
    """Return the staged script path appended to the command, or None when unset."""
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("script must be a path string")
    return value


def _optional_timeout(value: object) -> int | None:
    """Return the configured wall-clock limit, or None when no limit applies."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("timeout_seconds must be an integer number of seconds")
    try:
        seconds = int(value)
    except ValueError as exc:
        raise ValueError("timeout_seconds must be an integer number of seconds") from exc
    return seconds if seconds > 0 else None


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

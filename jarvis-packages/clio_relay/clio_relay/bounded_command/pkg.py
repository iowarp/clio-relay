"""JARVIS-CD package for bounded relay commands."""

from __future__ import annotations

import os
import queue
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from jarvis_cd.core.pkg import Application

from clio_relay.bounded_command.progress import adapter_from_config, append_progress_record

PROGRESS_FILE_ENV = "CLIO_RELAY_PROGRESS_FILE"
PROGRESS_TOKEN_ENV = "CLIO_RELAY_PROGRESS_TOKEN"


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
        if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
            raise ValueError("command must be a string array")
        env = os.environ.copy()
        supplied_env = self.config.get("env", {})
        if isinstance(supplied_env, dict):
            env.update({str(key): str(value) for key, value in supplied_env.items()})
        env.pop(PROGRESS_FILE_ENV, None)
        env.pop(PROGRESS_TOKEN_ENV, None)
        workdir_value = self.config.get("workdir")
        workdir = Path(workdir_value) if isinstance(workdir_value, str) else None
        timeout_value = self.config.get("timeout_seconds")
        timeout = int(timeout_value) if timeout_value is not None else None
        result = _run_streaming(
            command,
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
        start_new_session=os.name != "nt",
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    output_queue: queue.Queue[tuple[str, str | None]] = queue.Queue()

    def read_stream(name: str, stream: Any) -> None:
        try:
            for line in stream:
                output_queue.put((name, line))
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
    try:
        while len(closed_streams) < 2:
            if deadline is not None and time.monotonic() >= deadline:
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
            if stream_name == "stdout":
                stdout_chunks.append(line)
                print(line, end="", flush=True)
                if adapter is not None:
                    for record in adapter.observe_stdout(line):
                        append_progress_record(record)
            else:
                stderr_chunks.append(line)
                print(line, end="", file=sys.stderr, flush=True)
        returncode = process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        _terminate_process_tree(process)
        stdout, stderr = process.communicate()
        stdout_chunks.append(stdout)
        stderr_chunks.append(stderr)
        raise
    return subprocess.CompletedProcess(
        command,
        returncode,
        stdout="".join(stdout_chunks),
        stderr="".join(stderr_chunks),
    )


def _terminate_process_tree(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.send_signal(signal.CTRL_BREAK_EVENT)
        try:
            process.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            process.kill()
            return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return

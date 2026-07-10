"""Minimal stdio MCP client used by the relay MCP-call JARVIS package."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import threading
import time
from importlib import metadata
from pathlib import Path
from queue import Empty, Queue
from typing import Any


def run_mcp_call_from_params(params: dict[str, Any]) -> int:
    """Run a single MCP tools/call request and write mcp-result.json."""
    server = _required_str(params, "server")
    server_args = _str_list(params.get("server_args", []), key="server_args")
    tool = _required_str(params, "tool")
    arguments = _object(params.get("arguments", {}))
    timeout = _optional_int(params.get("timeout_seconds"))
    started_at = time.time()
    result_path = Path.cwd() / "mcp-result.json"
    try:
        command = [_resolve_executable(server), *server_args]
        process = _run_mcp_session(
            command,
            tool=tool,
            arguments=arguments,
            timeout=timeout,
        )
        returncode = process.returncode
        timed_out = False
        protocol_error = _protocol_error(process.stdout)
        if protocol_error is not None and returncode == 0:
            returncode = 1
    except subprocess.TimeoutExpired as exc:
        process = subprocess.CompletedProcess(
            args=[_resolve_executable(server), *server_args],
            returncode=124,
            stdout=exc.stdout or "",
            stderr=exc.stderr or "",
        )
        returncode = 124
        timed_out = True
        protocol_error = None
    _write_mcp_result(
        result_path=result_path,
        server=server,
        server_args=server_args,
        tool=tool,
        arguments=arguments,
        returncode=returncode,
        stdout=str(process.stdout or ""),
        stderr=str(process.stderr or ""),
        started_at=started_at,
        timed_out=timed_out,
        protocol_error=protocol_error,
    )
    return returncode


def _initialize_message() -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": "clio-relay-mcp-init",
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "clio-relay", "version": _package_version()},
        },
    }


def _initialized_message() -> dict[str, Any]:
    return {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}


def _call_message(*, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": "clio-relay-mcp-call",
        "method": "tools/call",
        "params": {"name": tool, "arguments": arguments},
    }


def _render_session_input(*, tool: str, arguments: dict[str, Any]) -> str:
    messages = (
        _initialize_message(),
        _initialized_message(),
        _call_message(tool=tool, arguments=arguments),
    )
    return "\n".join(json.dumps(item, separators=(",", ":")) for item in messages) + "\n"


def _package_version() -> str:
    try:
        return metadata.version("clio-relay")
    except metadata.PackageNotFoundError:
        return "0+unknown"


def _protocol_error(stdout: str) -> str | None:
    call_seen = False
    for line in stdout.splitlines():
        if not line.strip():
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("id") != "clio-relay-mcp-call":
            continue
        call_seen = True
        error = message.get("error")
        if error is not None:
            return json.dumps(error, sort_keys=True)
    if not call_seen:
        return "missing tools/call response"
    return None


def _write_mcp_result(
    *,
    result_path: Path,
    server: str,
    server_args: list[str],
    tool: str,
    arguments: dict[str, Any],
    returncode: int,
    stdout: str,
    stderr: str,
    started_at: float,
    timed_out: bool,
    protocol_error: str | None,
) -> None:
    finished_at = time.time()
    result_path.write_text(
        json.dumps(
            {
                "server": server,
                "server_args": server_args,
                "tool": tool,
                "arguments": arguments,
                "returncode": returncode,
                "stdout": stdout,
                "stderr": stderr,
                "timed_out": timed_out,
                "protocol_error": protocol_error,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_seconds": finished_at - started_at,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _run_mcp_session(
    command: list[str],
    *,
    tool: str,
    arguments: dict[str, Any],
    timeout: int | None,
) -> subprocess.CompletedProcess[str]:
    process = _open_process(command)
    stdout_queue: Queue[str | None] = Queue()
    stderr_queue: Queue[str | None] = Queue()
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    stdout_thread = _start_reader(process.stdout, stdout_queue)
    stderr_thread = _start_reader(process.stderr, stderr_queue)
    started_at = time.monotonic()
    deadline = None if timeout is None else started_at + timeout
    try:
        _write_message(process, _initialize_message())
        _wait_for_response(
            stdout_queue,
            "clio-relay-mcp-init",
            stdout_lines,
            deadline=deadline,
            command=command,
        )
        _write_message(process, _initialized_message())
        _write_message(process, _call_message(tool=tool, arguments=arguments))
        _wait_for_response(
            stdout_queue,
            "clio-relay-mcp-call",
            stdout_lines,
            deadline=deadline,
            command=command,
        )
        if process.stdin is not None:
            process.stdin.close()
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        process.wait(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        _terminate_process_tree(process)
        _drain_available(stdout_queue, stdout_lines)
        _drain_available(stderr_queue, stderr_lines)
        raise subprocess.TimeoutExpired(
            command,
            timeout if timeout is not None else 0,
            output="".join(stdout_lines) or exc.output,
            stderr="".join(stderr_lines) or exc.stderr,
        ) from exc
    finally:
        _join_reader(stdout_thread, stdout_queue, stdout_lines)
        _join_reader(stderr_thread, stderr_queue, stderr_lines)
    return subprocess.CompletedProcess(
        command,
        process.returncode if process.returncode is not None else 0,
        stdout="".join(stdout_lines),
        stderr="".join(stderr_lines),
    )


def _write_message(process: subprocess.Popen[str], message: dict[str, Any]) -> None:
    if process.stdin is None:
        raise RuntimeError("MCP server stdin is not available")
    process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    process.stdin.flush()


def _wait_for_response(
    queue: Queue[str | None],
    response_id: str,
    lines: list[str],
    *,
    deadline: float | None,
    command: list[str],
) -> None:
    while True:
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise subprocess.TimeoutExpired(command, timeout=0, output="".join(lines))
        try:
            line = queue.get(timeout=0.2 if remaining is None else min(0.2, remaining))
        except Empty:
            continue
        if line is None:
            raise subprocess.TimeoutExpired(command, timeout=0, output="".join(lines))
        lines.append(line)
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("id") == response_id:
            return


def _start_reader(stream: Any, queue: Queue[str | None]) -> threading.Thread:
    def read_stream() -> None:
        try:
            if stream is not None:
                for line in stream:
                    queue.put(line)
        finally:
            queue.put(None)

    thread = threading.Thread(target=read_stream, daemon=True)
    thread.start()
    return thread


def _join_reader(
    thread: threading.Thread,
    queue: Queue[str | None],
    lines: list[str],
) -> None:
    thread.join(timeout=1)
    _drain_available(queue, lines)


def _drain_available(queue: Queue[str | None], lines: list[str]) -> None:
    while True:
        try:
            line = queue.get_nowait()
        except Empty:
            return
        if line is not None:
            lines.append(line)


def _run_process(
    command: list[str],
    *,
    input: str,
    timeout: int | None,
) -> subprocess.CompletedProcess[str]:
    process = _open_process(command)
    try:
        stdout, stderr = process.communicate(input=input, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _terminate_process_tree(process)
        stdout, stderr = process.communicate()
        raise subprocess.TimeoutExpired(
            command,
            timeout if timeout is not None else 0,
            output=stdout or exc.stdout,
            stderr=stderr or exc.stderr,
        ) from exc
    return subprocess.CompletedProcess(command, process.returncode, stdout=stdout, stderr=stderr)


def _open_process(command: list[str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        command,
        env=_scrubbed_env(),
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=os.name != "nt",
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )


def _resolve_executable(executable: str) -> str:
    """Resolve executables commonly installed into user-local cluster paths."""
    resolved = shutil.which(executable)
    if resolved is not None:
        return resolved
    if executable == "uvx":
        user_local_uvx = Path.home() / ".local" / "bin" / "uvx"
        if user_local_uvx.exists():
            return str(user_local_uvx)
    return executable


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


def _scrubbed_env() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("CLIO_RELAY_PROGRESS_FILE", None)
    env.pop("CLIO_RELAY_PROGRESS_TOKEN", None)
    return env


def _required_str(params: dict[str, Any], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("arguments must be an object")
    return value


def _str_list(value: Any, *, key: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key} must be a string array")
    return value


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)

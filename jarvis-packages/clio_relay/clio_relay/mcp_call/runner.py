"""Minimal stdio MCP client used by the relay MCP-call JARVIS package."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any


def run_mcp_call_from_params(params: dict[str, Any]) -> int:
    """Run a single MCP tools/call request and write mcp-result.json."""
    server = _required_str(params, "server")
    tool = _required_str(params, "tool")
    arguments = _object(params.get("arguments", {}))
    timeout = _optional_int(params.get("timeout_seconds"))
    started_at = time.time()
    result_path = Path.cwd() / "mcp-result.json"
    try:
        process = _run_process(
            [server],
            input=_render_session_input(tool=tool, arguments=arguments),
            timeout=timeout,
        )
        returncode = process.returncode
        timed_out = False
        protocol_error = _protocol_error(process.stdout)
        if protocol_error is not None and returncode == 0:
            returncode = 1
    except subprocess.TimeoutExpired as exc:
        process = subprocess.CompletedProcess(
            args=[server],
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


def _render_session_input(*, tool: str, arguments: dict[str, Any]) -> str:
    initialize = {
        "jsonrpc": "2.0",
        "id": "clio-relay-mcp-init",
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "clio-relay", "version": "0.1.0"},
        },
    }
    initialized = {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
    call = {
        "jsonrpc": "2.0",
        "id": "clio-relay-mcp-call",
        "method": "tools/call",
        "params": {"name": tool, "arguments": arguments},
    }
    messages = (initialize, initialized, call)
    return "\n".join(json.dumps(item, separators=(",", ":")) for item in messages) + "\n"


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


def _run_process(
    command: list[str],
    *,
    input: str,
    timeout: int | None,
) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        command,
        env=_scrubbed_env(),
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=os.name != "nt",
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
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


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)

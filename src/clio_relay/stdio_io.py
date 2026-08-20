"""Bounded stdio message read/write primitives for one MCP child process.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). These
are pure I/O primitives -- none call back into another mcp_call owner module's
overridable surface, so they are safe to import normally (no facade reach-back
needed).
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
from collections.abc import Callable
from queue import Empty, Queue
from typing import Any

from clio_relay.constants import _STREAM_READ_CHARS
from clio_relay.protocol_messages import (
    _decoded_json_object,
    _McpProtocolFailure,
    _StreamEvent,
    _StreamLimit,
)


def _write_message(process: subprocess.Popen[str], message: dict[str, Any]) -> None:
    if process.stdin is None:
        raise RuntimeError("MCP server stdin is not available")
    process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    process.stdin.flush()


def _wait_for_response(
    queue: Queue[_StreamEvent],
    response_id: str,
    lines: list[str],
    *,
    process: subprocess.Popen[str],
    deadline: float | None,
    command: list[str],
    response_bytes: list[int] | None = None,
    max_response_bytes: int | None = None,
    response_label: str = "MCP response",
    notification_handler: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    while True:
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise subprocess.TimeoutExpired(command, timeout=0, output="".join(lines))
        try:
            line = queue.get(timeout=0.2 if remaining is None else min(0.2, remaining))
        except Empty:
            continue
        if line is None:
            returncode = process.poll()
            if returncode is None:
                try:
                    returncode = process.wait(timeout=0.2)
                except subprocess.TimeoutExpired:
                    returncode = None
            detail = f" with return code {returncode}" if returncode is not None else ""
            raise _McpProtocolFailure(
                f"MCP server stdout closed before response {response_id}{detail}"
            )
        if isinstance(line, _StreamLimit):
            raise _McpProtocolFailure(line.message)
        lines.append(line)
        if response_bytes is not None:
            response_bytes[0] += len(line.encode("utf-8"))
            if max_response_bytes is not None and response_bytes[0] > max_response_bytes:
                raise _McpProtocolFailure(
                    f"{response_label} exceeded maximum response size {max_response_bytes} bytes"
                )
        message = _decoded_json_object(line)
        if message is None:
            continue
        if notification_handler is not None and message.get("method") == "notifications/progress":
            notification_handler(message)
        if message.get("id") == response_id:
            return message


def _start_reader(
    stream: Any,
    queue: Queue[_StreamEvent],
    *,
    stream_name: str,
    max_bytes: int,
) -> threading.Thread:
    def read_stream() -> None:
        captured_bytes = 0
        pending = ""
        limit_reported = False
        try:
            if stream is not None:
                while True:
                    fragment = stream.readline(_STREAM_READ_CHARS)
                    if fragment == "":
                        break
                    if limit_reported:
                        continue
                    captured_bytes += len(fragment.encode("utf-8"))
                    if captured_bytes > max_bytes:
                        queue.put(
                            _StreamLimit(
                                f"MCP server {stream_name} exceeded maximum capture size "
                                f"{max_bytes} bytes"
                            )
                        )
                        pending = ""
                        limit_reported = True
                        continue
                    pending += fragment
                    if fragment.endswith("\n"):
                        queue.put(pending)
                        pending = ""
                if pending and not limit_reported:
                    queue.put(pending)
        finally:
            queue.put(None)

    thread = threading.Thread(target=read_stream, daemon=True)
    thread.start()
    return thread


def _join_reader(
    thread: threading.Thread,
    queue: Queue[_StreamEvent],
    lines: list[str],
) -> None:
    thread.join(timeout=1)
    _drain_available(queue, lines)


def _drain_available(queue: Queue[_StreamEvent], lines: list[str]) -> None:
    while True:
        try:
            line = queue.get_nowait()
        except Empty:
            return
        if isinstance(line, _StreamLimit):
            lines.append(f"\n[{line.message}]\n")
        elif line is not None:
            lines.append(line)

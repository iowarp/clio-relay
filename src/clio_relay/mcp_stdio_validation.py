"""Packaged stdio MCP boundary exercised by release acceptance commands."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any, cast

from clio_relay.errors import RelayError

JSON = dict[str, Any]
_INITIALIZE_ID = "clio-relay-validation-initialize"
_TOOLS_LIST_ID = "clio-relay-validation-tools-list"
_TOOLS_CALL_ID = "clio-relay-validation-tools-call"


@dataclass(frozen=True)
class PackagedMcpStdioSession:
    """Machine evidence captured from one packaged MCP stdio process."""

    command: tuple[str, ...]
    returncode: int
    initialize_response: JSON
    tools_list_response: JSON
    tools_call_response: JSON
    transcript_sha256: str
    stderr_sha256: str
    stderr_excerpt: str

    def evidence(self) -> JSON:
        """Return bounded JSON evidence suitable for validation reports."""
        initialize_result = _mapping(self.initialize_response.get("result")) or {}
        return {
            "boundary": "packaged_clio_relay_mcp_server_stdio",
            "command": list(self.command),
            "returncode": self.returncode,
            "protocol_version": initialize_result.get("protocolVersion"),
            "server_info": initialize_result.get("serverInfo"),
            "initialize_response": self.initialize_response,
            "tools_list_response": self.tools_list_response,
            "tools_call_response": self.tools_call_response,
            "transcript_sha256": self.transcript_sha256,
            "stderr_sha256": self.stderr_sha256,
            "stderr_excerpt": self.stderr_excerpt,
        }


def run_packaged_mcp_stdio_session(
    *,
    profile: str,
    tool: str,
    arguments: JSON,
    timeout_seconds: float = 60,
) -> PackagedMcpStdioSession:
    """Initialize, list, and call through the installed ``clio-relay`` executable."""
    executable = shutil.which("clio-relay")
    if executable is None:
        raise RelayError(
            "packaged clio-relay executable is unavailable; run validation through uvx or "
            "an installed wheel"
        )
    command = (executable, "mcp-server", "--profile", profile)
    messages: tuple[JSON, ...] = (
        {
            "jsonrpc": "2.0",
            "id": _INITIALIZE_ID,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "clio-relay-validation", "version": "1.0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": _TOOLS_LIST_ID,
            "method": "tools/list",
            "params": {},
        },
        {
            "jsonrpc": "2.0",
            "id": _TOOLS_CALL_ID,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        },
    )
    session_input = "".join(
        json.dumps(message, sort_keys=True, separators=(",", ":")) + "\n" for message in messages
    )
    try:
        completed = subprocess.run(
            command,
            input=session_input,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RelayError(f"packaged MCP stdio validation failed: {exc}") from exc
    responses = _responses_by_id(completed.stdout)
    missing = [
        response_id
        for response_id in (_INITIALIZE_ID, _TOOLS_LIST_ID, _TOOLS_CALL_ID)
        if response_id not in responses
    ]
    if completed.returncode != 0 or missing:
        detail = completed.stderr.strip()[-1000:]
        raise RelayError(
            "packaged MCP stdio validation returned an incomplete transcript: "
            f"returncode={completed.returncode} missing={missing} stderr={detail!r}"
        )
    transcript = completed.stdout.encode("utf-8")
    stderr = completed.stderr.encode("utf-8")
    return PackagedMcpStdioSession(
        command=command,
        returncode=completed.returncode,
        initialize_response=responses[_INITIALIZE_ID],
        tools_list_response=responses[_TOOLS_LIST_ID],
        tools_call_response=responses[_TOOLS_CALL_ID],
        transcript_sha256=hashlib.sha256(transcript).hexdigest(),
        stderr_sha256=hashlib.sha256(stderr).hexdigest(),
        stderr_excerpt=completed.stderr[-4000:],
    )


def _responses_by_id(stdout: str) -> dict[str, JSON]:
    responses: dict[str, JSON] = {}
    for line in stdout.splitlines():
        try:
            decoded = cast(object, json.loads(line))
        except json.JSONDecodeError:
            continue
        if not isinstance(decoded, dict):
            continue
        response = cast(JSON, decoded)
        response_id = response.get("id")
        if isinstance(response_id, str):
            responses[response_id] = response
    return responses


def _mapping(value: object) -> JSON | None:
    return cast(JSON, value) if isinstance(value, dict) else None

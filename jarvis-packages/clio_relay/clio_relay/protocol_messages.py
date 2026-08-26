"""JSON-RPC message construction/parsing and bounded wire-value validation.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). Every
function here is a pure leaf: none call back into another mcp_call owner module's
overridable surface, so they are safe to import normally (no facade reach-back
needed). ``_McpProtocolFailure`` and ``_StreamLimit`` are the shared vocabulary
nearly every other mcp_call module raises/handles, so they live in this
zero-dependency module rather than in ``runner.py`` itself.
"""

from __future__ import annotations

import json
import math
from importlib import metadata
from typing import Any, cast


class _McpProtocolFailure(RuntimeError):
    """Bounded local failure while consuming an MCP protocol session."""


class _StreamLimit:
    """Marker emitted after a child stream exceeds its capture budget."""

    __slots__ = ("message",)

    def __init__(self, message: str) -> None:
        self.message = message


_StreamEvent = str | _StreamLimit | None


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


def _call_message(
    *,
    tool: str,
    arguments: dict[str, Any],
    progress_token: str | None = None,
    response_id: str = "clio-relay-mcp-call",
) -> dict[str, Any]:
    params: dict[str, Any] = {"name": tool, "arguments": arguments}
    if progress_token is not None:
        params["_meta"] = {"progressToken": progress_token}
    return {
        "jsonrpc": "2.0",
        "id": response_id,
        "method": "tools/call",
        "params": params,
    }


def _tools_list_message(
    *, cursor: str | None = None, response_id: str = "clio-relay-mcp-tools-list"
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if cursor is not None:
        params["cursor"] = cursor
    return {
        "jsonrpc": "2.0",
        "id": response_id,
        "method": "tools/list",
        "params": params,
    }


def _package_version() -> str:
    try:
        return metadata.version("clio-relay")
    except metadata.PackageNotFoundError:
        return "0+unknown"


def _decoded_json_object(value: str) -> dict[str, Any] | None:
    """Decode a JSON object without leaking decoder ``Unknown`` types."""
    try:
        decoded: object = json.loads(value)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, dict):
        return None
    return cast(dict[str, Any], decoded)


def _text_output(value: str | bytes | None) -> str:
    """Normalize subprocess timeout output from text or byte mode."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _protocol_error(stdout: str, *, operation: str = "tools/call") -> str | None:
    response_id = _response_id(operation)
    response_seen = False
    for line in stdout.splitlines():
        if not line.strip():
            continue
        message = _decoded_json_object(line)
        if message is None:
            continue
        message_id = message.get("id")
        matching_id = message_id == response_id or (
            operation == "tools/list"
            and isinstance(message_id, str)
            and message_id.startswith(f"{response_id}-page-")
        )
        if not matching_id:
            continue
        response_seen = True
        error = message.get("error")
        if error is not None:
            return json.dumps(error, sort_keys=True)
        result = message.get("result")
        if operation == "tools/call" and isinstance(result, dict):
            typed_result = cast(dict[str, Any], result)
            if typed_result.get("isError") is True:
                return "tools/call returned isError=true"
    if not response_seen:
        return f"missing {operation} response"
    return None


def _response_id(operation: str) -> str:
    if operation == "tools/call":
        return "clio-relay-mcp-call"
    if operation == "tools/list":
        return "clio-relay-mcp-tools-list"
    raise ValueError(f"unsupported MCP operation: {operation}")


def _response_result(stdout: str, *, response_id: str) -> dict[str, Any] | None:
    matched: dict[str, Any] | None = None
    for line in stdout.splitlines():
        if not line.strip():
            continue
        message = _decoded_json_object(line)
        if message is None or message.get("id") != response_id:
            continue
        result = message.get("result")
        matched = cast(dict[str, Any], result) if isinstance(result, dict) else None
    return matched


def structured_result_from_protocol_result(
    protocol_result: dict[str, Any] | None,
    *,
    operation: str,
) -> dict[str, Any] | None:
    """Decode one ``tools/call`` protocol result into its structured content.

    Public (clio-relay#271 direction): reused by ``mcp_call_result_error.py``
    (clio-relay#183 residual + #248) to structurally decode a typed tool
    error from ``protocol_result.content[*].text`` when the durable
    ``mcp-result.json`` document carries no top-level ``structured_result``
    key at all -- the exact shape a remote MCP server that never populates
    ``structuredContent`` (only the MCP-required ``content`` text item)
    produces. Only items carrying ``type: "text"`` are ever decoded, per
    the MCP content-block contract.
    """
    if operation != "tools/call" or protocol_result is None:
        return None
    structured = protocol_result.get("structuredContent")
    if isinstance(structured, dict):
        return cast(dict[str, Any], structured)
    content = protocol_result.get("content")
    if not isinstance(content, list):
        return None
    for raw_item in cast(list[object], content):
        if not isinstance(raw_item, dict):
            continue
        item = cast(dict[str, Any], raw_item)
        if item.get("type") != "text":
            continue
        text = item.get("text")
        if not isinstance(text, str):
            continue
        decoded = _decoded_json_object(text)
        if decoded is not None:
            return decoded
    return None


def _nonempty_bounded_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise _McpProtocolFailure(f"MCP package progress {field_name} was invalid")
    return value


def _finite_progress_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate producer keys before schema validation."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _bounded_finite_json(value: object, label: str, maximum: int) -> None:
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise _McpProtocolFailure(f"MCP {label} was not finite JSON") from exc
    if len(payload) > maximum:
        raise _McpProtocolFailure(f"MCP {label} exceeded its byte limit")

"""MCP transcript parsing and protocol/tools contract validation.

Extracted from :mod:`clio_relay.mcp_stdio_validation` (file-size
decomposition; see ``scripts/check_file_size.py``). This module owns
response-id correlation over the newline-delimited JSON-RPC transcript
(``_responses_by_id``) and the generic MCP protocol contract every packaged
stdio session must satisfy: the exact ``initialize``/``tools/list``/
``tools/call`` result shapes, the pinned protocol version and relay
``serverInfo``, and the user-profile static-tool exposure guard. The
JARVIS-specific pinned virtual-tool contract (a distinct, security-relevant
concern layered on top of a validated ``tools/list``) lives separately in
``mcp_stdio_validation_jarvis_contract.py``.

The four request/response id and protocol-version constants are re-exported
under their original names -- :mod:`clio_relay.mcp_stdio_validation` builds
the outgoing JSON-RPC request messages with them, and
``mcp_stdio_validation_process.py`` reads them while correlating responses
during the staged handshake exchange. The functions are private helpers;
``run_packaged_mcp_stdio_session`` and ``_exchange_staged_mcp`` (the latter
in ``mcp_stdio_validation_process.py``) import them directly, and
``tests/test_mcp_stdio_validation.py`` calls ``_responses_by_id`` directly on
the facade module object -- no callers monkeypatch this module's own names,
so a plain forwarding import is sufficient everywhere.
"""

from __future__ import annotations

import re
from typing import Any, cast

from clio_relay import __version__
from clio_relay.errors import RelayError
from clio_relay.jarvis_mcp import jarvis_user_contract
from clio_relay.mcp_server import USER_MCP_TOOL_NAMES, static_mcp_tool_names
from clio_relay.mcp_stdio_validation_support import _canonical_digest, _mapping, decode_strict_json

JSON = dict[str, Any]

_INITIALIZE_ID = "clio-relay-validation-initialize"
_TOOLS_LIST_ID = "clio-relay-validation-tools-list"
_TOOLS_CALL_ID = "clio-relay-validation-tools-call"
_EXPECTED_PROTOCOL_VERSION = "2024-11-05"


def _normalize_validation_profile(profile: str) -> str:
    """Normalize the exact aliases accepted by the packaged MCP command."""
    normalized = profile.strip().lower()
    if normalized in {"", "user", "agent"}:
        return "user"
    if normalized in {"admin", "operator", "all"}:
        return normalized
    raise RelayError("packaged MCP validation profile was unsupported")


def _responses_by_id(
    stdout: bytes,
    *,
    allowed_ids: set[str] | None = None,
) -> dict[str, JSON]:
    if not stdout or not stdout.endswith(b"\n"):
        raise RelayError("packaged MCP stdio transcript omitted its final LF frame boundary")
    frames = stdout[:-1].split(b"\n")
    if any(not frame for frame in frames):
        raise RelayError("packaged MCP stdio transcript contained a blank frame")
    accepted_ids = allowed_ids or {_INITIALIZE_ID, _TOOLS_LIST_ID, _TOOLS_CALL_ID}
    responses: dict[str, JSON] = {}
    for frame in frames:
        decoded = decode_strict_json(frame, label="packaged MCP stdio transcript frame")
        if not isinstance(decoded, dict):
            raise RelayError("packaged MCP stdio transcript contained a non-object message")
        response = cast(JSON, decoded)
        response_id = response.get("id")
        if not isinstance(response_id, str):
            if response_id is None and (
                response.get("jsonrpc") == "2.0"
                and isinstance(response.get("method"), str)
                and bool(response.get("method"))
                and set(response) <= {"jsonrpc", "method", "params"}
                and ("params" not in response or isinstance(response.get("params"), dict))
            ):
                continue
            raise RelayError("packaged MCP stdio transcript contained an uncorrelated message")
        if response_id not in accepted_ids:
            raise RelayError("packaged MCP stdio transcript contained an unknown response id")
        if response_id in responses:
            raise RelayError("packaged MCP stdio transcript repeated a response id")
        if response.get("jsonrpc") != "2.0":
            raise RelayError("packaged MCP stdio transcript used an unexpected JSON-RPC version")
        has_result = "result" in response
        has_error = "error" in response
        if (
            has_result == has_error
            or "method" in response
            or set(response) - {"jsonrpc", "id", "result", "error"}
        ):
            raise RelayError("packaged MCP stdio transcript contained an invalid response envelope")
        if has_error:
            error = _mapping(response.get("error"))
            if (
                error is None
                or set(error) - {"code", "message", "data"}
                or not isinstance(error.get("code"), int)
                or isinstance(error.get("code"), bool)
                or not isinstance(error.get("message"), str)
            ):
                raise RelayError("packaged MCP stdio transcript contained an invalid error object")
        responses[response_id] = response
    return responses


def _required_result(response: JSON, *, label: str) -> JSON:
    if "error" in response:
        raise RelayError(f"packaged MCP {label} returned a JSON-RPC error")
    result = _mapping(response.get("result"))
    if result is None:
        raise RelayError(f"packaged MCP {label} omitted its result object")
    return result


def _validate_protocol_contract(
    *,
    initialize_response: JSON,
    tools_list_response: JSON,
    called_tool: str,
    profile: str,
) -> tuple[JSON, list[JSON], JSON]:
    server_info = _validate_initialize_contract(initialize_response)
    tools, selected = _validate_tools_contract(
        tools_list_response,
        called_tool=called_tool,
        profile=profile,
    )
    return server_info, tools, selected


def _validate_initialize_contract(initialize_response: JSON) -> JSON:
    """Validate the exact packaged relay initialization contract before activation."""
    initialize_result = _required_result(initialize_response, label="initialize")
    if set(initialize_result) - {
        "protocolVersion",
        "capabilities",
        "serverInfo",
        "instructions",
    }:
        raise RelayError("packaged MCP initialize result contained unexpected fields")
    if initialize_result.get("protocolVersion") != _EXPECTED_PROTOCOL_VERSION:
        raise RelayError("packaged MCP initialize protocol version did not match")
    capabilities = _mapping(initialize_result.get("capabilities"))
    tools_capability = None if capabilities is None else _mapping(capabilities.get("tools"))
    if capabilities is None or tools_capability is None:
        raise RelayError("packaged MCP initialize capabilities did not match")
    if tools_capability.get("listChanged") is not True:
        raise RelayError("packaged MCP initialize did not advertise live tool catalogs")
    instructions = initialize_result.get("instructions")
    if not isinstance(instructions, str) or "sole execution" not in instructions:
        raise RelayError("packaged MCP initialize omitted relay execution authority")
    server_info = _mapping(initialize_result.get("serverInfo"))
    if server_info is None or server_info.get("name") != "clio-relay":
        raise RelayError("packaged MCP initialize serverInfo name did not match")
    if server_info.get("version") != __version__:
        raise RelayError(
            "packaged MCP initialize serverInfo version did not match the running distribution"
        )
    if set(server_info) != {"name", "version"}:
        raise RelayError("packaged MCP initialize serverInfo contained unexpected fields")
    return server_info


def _validate_tools_contract(
    tools_list_response: JSON,
    *,
    called_tool: str,
    profile: str,
) -> tuple[list[JSON], JSON]:
    """Validate tools/list before issuing the selected tools/call request."""
    tools_result = _required_result(tools_list_response, label="tools/list")
    if set(tools_result) - {"tools", "nextCursor", "_meta"}:
        raise RelayError("packaged MCP tools/list result contained unexpected fields")
    metadata = _mapping(tools_result.get("_meta"))
    if metadata is not None and (
        set(metadata)
        != {
            "clio-relay/remote-mcp-catalog-revision",
            "clio-relay/profile",
        }
        or metadata.get("clio-relay/profile") != profile
        or not isinstance(metadata.get("clio-relay/remote-mcp-catalog-revision"), str)
        or re.fullmatch(
            r"[0-9a-f]{64}",
            cast(str, metadata.get("clio-relay/remote-mcp-catalog-revision")),
        )
        is None
    ):
        raise RelayError("packaged MCP tools/list metadata did not match its exact contract")
    if tools_result.get("nextCursor") is not None:
        raise RelayError("packaged MCP tools/list was paginated and therefore incomplete")
    raw_tools = tools_result.get("tools")
    if not isinstance(raw_tools, list):
        raise RelayError("packaged MCP tools/list omitted its tools array")
    tools: list[JSON] = []
    names: set[str] = set()
    selected: JSON | None = None
    for raw_tool in cast(list[object], raw_tools):
        if not isinstance(raw_tool, dict):
            raise RelayError("packaged MCP tools/list contained a non-object tool")
        definition = cast(JSON, raw_tool)
        name = definition.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise RelayError("packaged MCP tools/list contained an invalid or duplicate tool name")
        if not isinstance(definition.get("description"), str) or not isinstance(
            definition.get("inputSchema"), dict
        ):
            raise RelayError(f"packaged MCP tool {name} omitted its exact agent-facing schema")
        names.add(name)
        tools.append(definition)
        if name == called_tool:
            selected = definition
    if selected is None:
        raise RelayError(f"packaged MCP did not advertise required tool {called_tool}")
    if profile == "user":
        forbidden_static = (
            static_mcp_tool_names() - USER_MCP_TOOL_NAMES - set(jarvis_user_contract())
        )
        leaked = sorted(names & forbidden_static)
        if leaked:
            raise RelayError("packaged user MCP exposed static administrative tools")
    _canonical_digest({"tools": tools})
    return tools, selected


def _safe_call_job_id(response: JSON) -> str | None:
    """Project only one bounded non-secret relay job identifier from a call result."""
    try:
        structured = _validated_call_structured_content(response)
    except RelayError:
        return None
    candidate = structured.get("job_id")
    if (
        isinstance(candidate, str)
        and len(candidate) <= 1_024
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", candidate) is not None
    ):
        return candidate
    return None


def _validated_call_structured_content(response: JSON) -> JSON:
    """Validate one exact successful MCP result before projecting any durable identifier."""
    result = _required_result(response, label="tools/call")
    if set(result) != {"content", "structuredContent", "isError"}:
        raise RelayError("packaged MCP tools/call result contained unexpected fields")
    if result.get("isError") is not False:
        raise RelayError("packaged MCP tools/call reported an error")
    raw_content = result.get("content")
    structured = _mapping(result.get("structuredContent"))
    if not isinstance(raw_content, list) or structured is None:
        raise RelayError("packaged MCP tools/call omitted its exact structured result")
    content = cast(list[object], raw_content)
    if len(content) != 1 or not isinstance(content[0], dict):
        raise RelayError("packaged MCP tools/call returned invalid text content")
    item = cast(JSON, content[0])
    if set(item) != {"type", "text"}:
        raise RelayError("packaged MCP tools/call returned invalid text content")
    text = item.get("text")
    if item.get("type") != "text" or not isinstance(text, str):
        raise RelayError("packaged MCP tools/call returned invalid text content")
    if decode_strict_json(text, label="packaged MCP tools/call text") != structured:
        raise RelayError("packaged MCP tools/call text and structured content differed")
    return structured

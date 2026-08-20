"""Packaged MCP stdio child evidence: transport secrets and tool-call checks.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of running
one isolated packaged MCP child (its transport secrets scoped to that child
alone, never the parent process), validating its structured tool-call
result, and re-deriving the contract digests a
:class:`~clio_relay.mcp_stdio_validation.PackagedMcpStdioSession` already
observed so the acceptance evidence proves those digests rather than
merely repeating them.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Literal, cast

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.live_acceptance_models import PackagedMcpAcceptanceEvidence
from clio_relay.live_acceptance_secret_redaction import _redacted_text
from clio_relay.mcp_stdio_validation import PackagedMcpStdioSession, decode_strict_json
from clio_relay.validation_report import EvidenceReference, ValidationRecorder


@contextmanager
def _validation_check(
    recorder: ValidationRecorder,
    check_id: str,
    summary: str,
    *,
    forbidden_values: set[str],
) -> Generator[list[EvidenceReference]]:
    """Expose a typed check while redacting private values before failure recording."""
    with recorder.check(check_id, summary) as evidence:
        try:
            yield evidence
        except Exception as exc:
            original = str(exc)
            redacted = _redacted_text(original, forbidden_values)
            if redacted == original:
                raise
            raise RelayError(f"secure runtime operation failed: {redacted}") from None


def _configured_runtime_secret(
    *,
    explicit: str | None,
    environment_name: str,
    label: str,
) -> str:
    """Resolve one required runtime transport secret without echoing its value."""
    value = explicit if explicit is not None else os.environ.get(environment_name)
    if not value:
        raise ConfigurationError(
            f"secure runtime acceptance requires {label} in {environment_name}"
        )
    return value


@contextmanager
def _isolated_runtime_child_environment(
    *,
    token_name: str,
    token: str,
    secret_name: str,
    secret: str,
) -> Generator[dict[str, str]]:
    """Yield explicit transport values for one packaged child without parent mutation."""
    yield {token_name: token, secret_name: secret}


def _packaged_mcp_structured_result(
    session: PackagedMcpStdioSession,
    *,
    expected_tool: str,
) -> dict[str, Any]:
    """Validate one packaged MCP call and return its exact structured content."""
    tools_result = session.tools_list_response.get("result")
    if not isinstance(tools_result, dict):
        raise RelayError("packaged MCP tools/list omitted its result")
    tools_value = cast(dict[str, object], tools_result).get("tools")
    tools = cast(list[object], tools_value) if isinstance(tools_value, list) else []
    advertised_names = {
        cast(dict[str, object], tool).get("name") for tool in tools if isinstance(tool, dict)
    }
    if expected_tool not in advertised_names:
        raise RelayError(f"packaged MCP did not advertise required tool {expected_tool}")
    if "error" in session.tools_call_response:
        error_value = session.tools_call_response.get("error")
        error = cast(dict[str, object], error_value) if isinstance(error_value, dict) else {}
        message = error.get("message")
        raise RelayError(
            f"packaged MCP {expected_tool} failed: "
            f"{message if isinstance(message, str) else 'unknown error'}"
        )
    raw_result = session.tools_call_response.get("result")
    if not isinstance(raw_result, dict):
        raise RelayError(f"packaged MCP {expected_tool} omitted its result")
    result = cast(dict[str, Any], raw_result)
    if result.get("isError") is True:
        raise RelayError(f"packaged MCP {expected_tool} returned isError=true")
    structured = result.get("structuredContent")
    if not isinstance(structured, dict):
        raise RelayError(f"packaged MCP {expected_tool} omitted structuredContent")
    content_value = result.get("content")
    content = cast(list[object], content_value) if isinstance(content_value, list) else []
    if len(content) != 1:
        raise RelayError(f"packaged MCP {expected_tool} returned invalid text content")
    item = content[0]
    text = cast(dict[str, object], item).get("text") if isinstance(item, dict) else None
    if not isinstance(text, str):
        raise RelayError(f"packaged MCP {expected_tool} returned invalid text content")
    text_document = decode_strict_json(
        text,
        label=f"packaged MCP {expected_tool} text",
    )
    if text_document != structured:
        raise RelayError(f"packaged MCP {expected_tool} text and structured content differ")
    return {str(key): value for key, value in cast(dict[object, object], structured).items()}


def _packaged_mcp_acceptance_evidence(
    session: PackagedMcpStdioSession,
    *,
    expected_tool: str,
) -> PackagedMcpAcceptanceEvidence:
    """Recheck and copy identities observed by the installed MCP child process."""
    initialize_result = session.initialize_response.get("result")
    if not isinstance(initialize_result, dict):
        raise RelayError("packaged MCP initialize omitted its result")
    raw_server_info = cast(dict[str, object], initialize_result).get("serverInfo")
    if not isinstance(raw_server_info, dict):
        raise RelayError("packaged MCP initialize omitted observed serverInfo")
    server_info = cast(dict[str, object], raw_server_info)
    server_name = server_info.get("name")
    server_version = server_info.get("version")
    if server_name != "clio-relay" or not isinstance(server_version, str):
        raise RelayError("packaged MCP initialize returned invalid observed serverInfo")
    tools_result = session.tools_list_response.get("result")
    if not isinstance(tools_result, dict):
        raise RelayError("packaged MCP tools/list omitted its result")
    raw_tools = cast(dict[str, object], tools_result).get("tools")
    tools = cast(list[object], raw_tools) if isinstance(raw_tools, list) else []
    typed_tools = [cast(dict[str, Any], item) for item in tools if isinstance(item, dict)]
    selected = [tool for tool in typed_tools if tool.get("name") == expected_tool]
    if len(selected) != 1:
        raise RelayError("packaged MCP observed tool schema was not unique")
    configured = session.configured_executable
    canonical = session.canonical_executable
    digests = {
        "executable_sha256": session.executable_sha256,
        "server_info_sha256": session.server_info_sha256,
        "tools_list_sha256": session.tools_list_sha256,
        "called_tool_schema_sha256": session.called_tool_schema_sha256,
        "jarvis_virtual_tools_sha256": session.jarvis_virtual_tools_sha256,
    }
    if not configured or not canonical:
        raise RelayError("packaged MCP omitted its observed executable identity")
    containment_mode = session.containment_mode
    if (
        containment_mode not in {"windows_job_object", "linux_systemd_scope"}
        or not session.containment_enforceable
    ):
        raise RelayError("packaged MCP process containment was not enforceable")
    if not session.command or session.command[0] != canonical:
        raise RelayError("packaged MCP command did not use its observed canonical executable")
    if any(
        not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        for digest in digests.values()
    ):
        raise RelayError("packaged MCP omitted an observed contract digest")
    if session.server_info_sha256 != _packaged_mcp_canonical_sha256(server_info):
        raise RelayError("packaged MCP observed serverInfo digest changed")
    if session.tools_list_sha256 != _packaged_mcp_tools_sha256(typed_tools):
        raise RelayError("packaged MCP observed tools/list digest changed")
    if session.called_tool_schema_sha256 != _packaged_mcp_canonical_sha256(selected[0]):
        raise RelayError("packaged MCP observed called-tool schema digest changed")
    return PackagedMcpAcceptanceEvidence(
        command=list(session.command),
        configured_executable=configured,
        canonical_executable=canonical,
        executable_sha256=cast(str, session.executable_sha256),
        server_name="clio-relay",
        server_version=server_version,
        server_info_sha256=cast(str, session.server_info_sha256),
        tools_list_sha256=cast(str, session.tools_list_sha256),
        called_tool_schema_sha256=cast(str, session.called_tool_schema_sha256),
        jarvis_virtual_tools_sha256=cast(str, session.jarvis_virtual_tools_sha256),
        containment_mode=cast(
            Literal["windows_job_object", "linux_systemd_scope"],
            containment_mode,
        ),
        containment_enforceable=True,
    )


def _packaged_mcp_canonical_sha256(value: object) -> str:
    """Reproduce the packaged MCP helper's canonical contract digest."""
    payload = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _packaged_mcp_tools_sha256(tools: list[dict[str, Any]]) -> str:
    """Digest the exact sorted tools/list contract observed from stdio."""
    ordered = sorted(tools, key=lambda definition: cast(str, definition.get("name")))
    return _packaged_mcp_canonical_sha256({"tools": ordered})

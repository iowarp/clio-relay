"""Packaged relay MCP stdio evidence: initialize/list/call extraction.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns the three small predicates that
read one packaged stdio evidence document (a locally captured
``initialize``/``tools/list``/``tools/call`` transcript, or its newer bounded
protocol-evidence-digest projection) without trusting its shape: whether the
handshake passed, which tool names the listing advertised, and which durable
job id a call bound to. ``_as_json`` is the shared narrowing helper every
other remote-MCP validator owner module in this split imports back from
here.

None of these four names have a caller outside ``remote_mcp.py`` (confirmed
by grep before the move; other split owner modules import them directly from
here, not from ``remote_mcp.py``), so ``remote_mcp.py`` imports them directly
rather than re-exporting them.

``_stdio_initialize_passed`` reads ``CLIO_KIT_JARVIS_USER_TOOL_NAMES``,
``CLIO_KIT_JARVIS_USER_CONTRACT_ID``, and ``CLIO_KIT_JARVIS_USER_CONTRACT_SHA256``,
three of the contract-pin constants that still live in ``remote_mcp.py``
(unsequenced, post-campaign per the design doc). A module-scope import back
into ``remote_mcp.py`` (which imports this module for its own private-name
access) would be a load-order circular import; importing them inside the
function body instead is the proven idiom for that shape (see
``remote_mcp_wire_schemas.py``'s own ``virtual_jarvis_job_output_schema``).
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, cast

JSON = dict[str, Any]


def _stdio_initialize_passed(evidence: JSON | None) -> bool:
    if evidence is None:
        return True
    if "protocol_evidence_sha256" in evidence:
        projected = dict(evidence)
        expected_digest = projected.pop("protocol_evidence_sha256", None)
        try:
            observed_digest = hashlib.sha256(
                json.dumps(
                    projected,
                    allow_nan=False,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        except (TypeError, ValueError):
            return False
        digest_fields = (
            "executable_sha256",
            "server_info_sha256",
            "tools_list_sha256",
            "called_tool_schema_sha256",
            "jarvis_contract_sha256",
        )
        raw_tool_names = evidence.get("tool_names")
        if not isinstance(raw_tool_names, list) or not all(
            isinstance(name, str) and name for name in cast(list[object], raw_tool_names)
        ):
            return False
        tool_names = set(cast(list[str], raw_tool_names))
        from clio_relay.remote_mcp import (
            CLIO_KIT_JARVIS_USER_CONTRACT_ID,
            CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
            CLIO_KIT_JARVIS_USER_TOOL_NAMES,
        )

        advertised_jarvis_names = tool_names & CLIO_KIT_JARVIS_USER_TOOL_NAMES
        jarvis_surface_sha256 = evidence.get("jarvis_virtual_tools_sha256")
        jarvis_surface_valid = (not advertised_jarvis_names and jarvis_surface_sha256 is None) or (
            advertised_jarvis_names == set(CLIO_KIT_JARVIS_USER_TOOL_NAMES)
            and isinstance(jarvis_surface_sha256, str)
            and re.fullmatch(r"[0-9a-f]{64}", jarvis_surface_sha256) is not None
        )
        return (
            evidence.get("schema_version") == "clio-relay.packaged-mcp-stdio-evidence.v1"
            and evidence.get("boundary") == "packaged_clio_relay_mcp_server_stdio"
            and evidence.get("returncode") == 0
            and evidence.get("protocol_version") == "2024-11-05"
            and evidence.get("server_name") == "clio-relay"
            and isinstance(evidence.get("server_version"), str)
            and bool(evidence.get("server_version"))
            and isinstance(evidence.get("containment_enforceable"), bool)
            and isinstance(evidence.get("containment_mode"), str)
            and evidence.get("jarvis_contract_id") == CLIO_KIT_JARVIS_USER_CONTRACT_ID
            and evidence.get("jarvis_contract_sha256") == CLIO_KIT_JARVIS_USER_CONTRACT_SHA256
            and jarvis_surface_valid
            and expected_digest == observed_digest
            and all(
                isinstance(evidence.get(name), str)
                and re.fullmatch(r"[0-9a-f]{64}", cast(str, evidence[name])) is not None
                for name in digest_fields
            )
        )
    response = _as_json(evidence.get("initialize_response"))
    if response is None or response.get("error") is not None:
        return False
    result = _as_json(response.get("result"))
    if result is None:
        return False
    server_info = _as_json(result.get("serverInfo"))
    return (
        evidence.get("boundary") == "packaged_clio_relay_mcp_server_stdio"
        and evidence.get("returncode") == 0
        and isinstance(result.get("protocolVersion"), str)
        and server_info is not None
        and server_info.get("name") == "clio-relay"
    )


def _stdio_listed_tool_names(evidence: JSON | None) -> set[str]:
    if evidence is None:
        return set()
    if "protocol_evidence_sha256" in evidence:
        raw_names = evidence.get("tool_names")
        if not isinstance(raw_names, list):
            return set()
        typed_names = cast(list[object], raw_names)
        if not all(isinstance(name, str) and name for name in typed_names):
            return set()
        return set(cast(list[str], typed_names))
    response = _as_json(evidence.get("tools_list_response"))
    result = _as_json(response.get("result")) if response is not None else None
    tools = result.get("tools") if result is not None else None
    if not isinstance(tools, list):
        return set()
    listed_names: set[str] = set()
    for value in cast(list[object], tools):
        tool = _as_json(value)
        if tool is not None and isinstance(tool.get("name"), str):
            listed_names.add(cast(str, tool["name"]))
    return listed_names


def _stdio_call_job_id(evidence: JSON | None) -> str | None:
    if evidence is None:
        return None
    if "protocol_evidence_sha256" in evidence:
        job_id = evidence.get("call_job_id")
        return job_id if isinstance(job_id, str) else None
    response = _as_json(evidence.get("tools_call_response"))
    result = _as_json(response.get("result")) if response is not None else None
    if result is None:
        return None
    structured = _as_json(result.get("structuredContent"))
    if structured is not None and isinstance(structured.get("job_id"), str):
        return cast(str, structured["job_id"])
    content = result.get("content")
    if not isinstance(content, list):
        return None
    for value in cast(list[object], content):
        item = _as_json(value)
        if item is None or item.get("type") != "text":
            continue
        text = item.get("text")
        if not isinstance(text, str):
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        typed_payload = _as_json(payload)
        if typed_payload is not None and isinstance(typed_payload.get("job_id"), str):
            return cast(str, typed_payload["job_id"])
    return None


def _as_json(value: object) -> JSON | None:
    return cast(JSON, value) if isinstance(value, dict) else None

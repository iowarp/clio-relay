"""Packaged stdio MCP boundary exercised by release acceptance commands.

File-size decomposition (see ``scripts/check_file_size.py``): this module is
now a thin facade over six owner modules, each covering one concern of the
packaged stdio session lifecycle --

* ``mcp_stdio_validation_executable.py`` -- resolve and re-verify the exact
  installed ``clio-relay`` binary before and after exec.
* ``mcp_stdio_validation_process_io.py`` -- leaf process primitives: bounded
  pipe capture, launch-environment scrubbing, pipe-reader threads, teardown.
* ``mcp_stdio_validation_process.py`` -- the deadline-driven bounded spawn
  (``_run_bounded_process``) and the staged MCP handshake exchange it drives.
* ``mcp_stdio_validation_contract.py`` -- transcript response-id correlation
  and the generic MCP protocol/tools contract.
* ``mcp_stdio_validation_jarvis_contract.py`` -- the pinned JARVIS v3.6
  virtual-tool contract layered on top of a validated ``tools/list``.
* ``mcp_stdio_validation_support.py`` -- strict JSON decode, canonical
  digests, and diagnostic sanitization shared by the modules above.

This module itself keeps ``PackagedMcpStdioSession`` (the public evidence
record) and ``run_packaged_mcp_stdio_session`` (the public orchestrator)
resident: ``cli.py``, ``cli_jarvis_mcp_validate.py``, and
``remote_mcp_validation.py`` each do
``import clio_relay.mcp_stdio_validation as mcp_stdio_validation`` and then
call ``mcp_stdio_validation.run_packaged_mcp_stdio_session(...)`` as a
qualified attribute lookup -- several tests monkeypatch exactly that
attribute (``monkeypatch.setattr(mcp_stdio_validation, "run_packaged_mcp_
stdio_session", ...)``), which only keeps working if the function stays
physically defined here rather than merely re-exported. ``decode_strict_
json`` and ``_packaged_launch_environment`` are re-exported via qualified
assignment (not a plain ``from ... import``, which ruff would flag as
unused) purely so external importers and one direct test call-through keep
resolving; every other owner-module import below is used in this module's
own body.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from clio_relay import (
    mcp_stdio_validation_process_io,
    mcp_stdio_validation_support,
)
from clio_relay.errors import RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_ID,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
)
from clio_relay.mcp_stdio_validation_contract import (
    _EXPECTED_PROTOCOL_VERSION,
    _INITIALIZE_ID,
    _TOOLS_CALL_ID,
    _TOOLS_LIST_ID,
    _normalize_validation_profile,
    _responses_by_id,
    _safe_call_job_id,
    _validate_protocol_contract,
)
from clio_relay.mcp_stdio_validation_executable import (
    _resolve_packaged_executable,
    _verify_executable_unchanged,
)
from clio_relay.mcp_stdio_validation_jarvis_contract import _validate_pinned_jarvis_contract
from clio_relay.mcp_stdio_validation_process import _run_bounded_process
from clio_relay.mcp_stdio_validation_support import (
    _canonical_digest,
    _mapping,
    _sanitized_diagnostic,
    _tools_digest,
)

# ruff's unused-import check has no equivalent for a plain module-level
# assignment, unlike the `from ... import` it kept stripping as dead --
# neither name below has a reader in this module's own body (see the
# module docstring: both are re-exported for external importers/tests only).
decode_strict_json = mcp_stdio_validation_support.decode_strict_json
_packaged_launch_environment = mcp_stdio_validation_process_io._packaged_launch_environment

JSON = dict[str, Any]


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
    configured_executable: str | None = None
    canonical_executable: str | None = None
    executable_sha256: str | None = None
    server_info_sha256: str | None = None
    tools_list_sha256: str | None = None
    called_tool_schema_sha256: str | None = None
    jarvis_virtual_tools_sha256: str | None = None
    called_tool_name: str | None = None
    containment_mode: str | None = None
    containment_enforceable: bool = False

    def evidence(self) -> JSON:
        """Return bounded JSON evidence suitable for validation reports."""
        initialize_result = _mapping(self.initialize_response.get("result")) or {}
        server_info = _mapping(initialize_result.get("serverInfo")) or {}
        tools_result = _mapping(self.tools_list_response.get("result")) or {}
        raw_tools = tools_result.get("tools")
        tools = cast(list[object], raw_tools) if isinstance(raw_tools, list) else []
        tool_names: list[str] = []
        for raw_tool in tools:
            if not isinstance(raw_tool, dict):
                continue
            tool = cast(JSON, raw_tool)
            name = tool.get("name")
            if isinstance(name, str):
                tool_names.append(name)
        tool_names.sort()
        call_job_id = _safe_call_job_id(self.tools_call_response)
        projection: JSON = {
            "schema_version": "clio-relay.packaged-mcp-stdio-evidence.v1",
            "boundary": "packaged_clio_relay_mcp_server_stdio",
            "command": list(self.command),
            "configured_executable": self.configured_executable,
            "canonical_executable": self.canonical_executable,
            "executable_sha256": self.executable_sha256,
            "returncode": self.returncode,
            "protocol_version": initialize_result.get("protocolVersion"),
            "server_name": server_info.get("name"),
            "server_version": server_info.get("version"),
            "server_info_sha256": self.server_info_sha256,
            "tool_names": tool_names,
            "tools_list_sha256": self.tools_list_sha256,
            "called_tool_name": self.called_tool_name,
            "called_tool_schema_sha256": self.called_tool_schema_sha256,
            "call_job_id": call_job_id,
            "jarvis_contract_id": CLIO_KIT_JARVIS_USER_CONTRACT_ID,
            "jarvis_contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
            "jarvis_virtual_tools_sha256": self.jarvis_virtual_tools_sha256,
            "containment_mode": self.containment_mode,
            "containment_enforceable": self.containment_enforceable,
        }
        projection["protocol_evidence_sha256"] = _canonical_digest(projection)
        return projection


def run_packaged_mcp_stdio_session(
    *,
    profile: str,
    tool: str,
    arguments: JSON,
    timeout_seconds: float = 60,
    extra_environment: Mapping[str, str] | None = None,
    require_enforceable_containment: bool = False,
) -> PackagedMcpStdioSession:
    """Initialize, list, and call through the exact installed relay executable."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise RelayError("packaged MCP stdio validation timeout must be finite and positive")
    normalized_profile = _normalize_validation_profile(profile)
    executable = _resolve_packaged_executable()
    command = (str(executable.canonical_path), "mcp-server", "--profile", normalized_profile)
    messages: tuple[JSON, ...] = (
        {
            "jsonrpc": "2.0",
            "id": _INITIALIZE_ID,
            "method": "initialize",
            "params": {
                "protocolVersion": _EXPECTED_PROTOCOL_VERSION,
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
    try:
        session_input = "".join(
            json.dumps(
                message,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for message in messages
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RelayError("packaged MCP stdio request was not finite JSON") from exc
    stdout, stderr, returncode, containment = _run_bounded_process(
        command,
        session_input=session_input,
        timeout_seconds=timeout_seconds,
        extra_environment=extra_environment,
        require_enforceable_containment=require_enforceable_containment,
        staged_mcp=True,
        called_tool=tool,
        profile=normalized_profile,
    )
    _verify_executable_unchanged(executable)
    responses = _responses_by_id(stdout)
    missing = [
        response_id
        for response_id in (_INITIALIZE_ID, _TOOLS_LIST_ID, _TOOLS_CALL_ID)
        if response_id not in responses
    ]
    if returncode != 0 or missing:
        detail = _sanitized_diagnostic(
            stderr,
            forbidden_values=(extra_environment.values() if extra_environment else ()),
        )
        raise RelayError(
            "packaged MCP stdio validation returned an incomplete transcript: "
            f"returncode={returncode} missing={missing} stderr={detail!r}"
        )
    initialize = responses[_INITIALIZE_ID]
    tools_list = responses[_TOOLS_LIST_ID]
    server_info, tools, called_tool = _validate_protocol_contract(
        initialize_response=initialize,
        tools_list_response=tools_list,
        called_tool=tool,
        profile=normalized_profile,
    )
    jarvis_virtual_tools_sha256 = _validate_pinned_jarvis_contract(tools)
    return PackagedMcpStdioSession(
        command=command,
        returncode=returncode,
        initialize_response=initialize,
        tools_list_response=tools_list,
        tools_call_response=responses[_TOOLS_CALL_ID],
        transcript_sha256=hashlib.sha256(stdout).hexdigest(),
        stderr_sha256=hashlib.sha256(stderr).hexdigest(),
        stderr_excerpt=_sanitized_diagnostic(
            stderr,
            forbidden_values=(extra_environment.values() if extra_environment else ()),
        ),
        configured_executable=str(executable.configured_path),
        canonical_executable=str(executable.canonical_path),
        executable_sha256=executable.sha256,
        server_info_sha256=_canonical_digest(server_info),
        tools_list_sha256=_tools_digest(tools),
        called_tool_schema_sha256=_canonical_digest(called_tool),
        jarvis_virtual_tools_sha256=jarvis_virtual_tools_sha256,
        called_tool_name=tool,
        containment_mode=cast(str | None, containment.get("mode")),
        containment_enforceable=containment.get("enforceable") is True,
    )

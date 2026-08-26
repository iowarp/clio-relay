"""Discovered remote MCP tool schema, provenance, and identity verification.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5). This module owns the
foundational, bounded representation of one tool a remote MCP server
advertised (:class:`RemoteMcpToolSchema`), the durable evidence tying a
discovery to the artifact that produced it (:class:`RemoteMcpDiscoveryProvenance`),
the wire-object parser that turns an untrusted ``tools/list`` entry into a
validated :class:`RemoteMcpToolSchema`, and the small identity/verification
predicates (SHA-256 shape, verified-executable server artifacts, immutable
install provenance) that later concerns -- the schema cache, catalog
assembly, and acceptance reporting -- all build on.

:mod:`clio_relay.remote_mcp` re-exports :class:`RemoteMcpToolSchema`,
:class:`RemoteMcpDiscoveryProvenance`, and :func:`is_remote_mcp_control_query`
under their original names (external callers across several modules and
tests import them directly from ``clio_relay.remote_mcp``). The remaining
names here -- ``_parse_remote_tool``, ``_is_sha256``,
``_server_artifact_verified``, ``_immutable_remote_mcp_install_verified``,
and ``_stable_digest`` -- are private helpers with no callers outside
``remote_mcp.py`` (confirmed by grep before the move; other modules define
their own independent, non-imported same-named helpers, the same
no-shared-import discipline as ``process_containment.py``), so
``remote_mcp.py`` imports them directly rather than re-exporting them.

:func:`resolve_remote_tool_title` is the one exception to that no-shared-
import discipline (iowarp/clio-relay#164 repair round): it has a genuine
second caller outside this cluster, ``jarvis_mcp_validation_contract.py``'s
``_remote_contract_tool``, which parses the same untrusted ``tools/list``
shape into a :class:`RemoteMcpToolSchema` for the JARVIS remote-contract
check. Both that path and :func:`_parse_remote_tool` below feed
``remote_mcp_schema_digest`` compared against the same pinned contract sha
for the same live server, so they must resolve title identically or they
silently disagree about that server's schema digest. Kept unprefixed and
imported directly (not re-exported through ``remote_mcp.py``) since it is
still a narrow, two-caller helper, not a broad public API.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.remote_mcp_schema_validation import (
    _require_bounded_json_structure,
    _require_finite_json,
    _validate_json_schema,
)

JSON = dict[str, Any]

REMOTE_MCP_CACHE_SOURCE = "durable_relay_mcp_tools_list"
MAX_REMOTE_MCP_TOOL_SCHEMA_BYTES = 1024 * 1024
MAX_REMOTE_MCP_PROVENANCE_BYTES = 1024 * 1024


class RemoteMcpToolSchema(BaseModel):
    """Validated tool contract returned by a remote MCP ``tools/list`` call."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(max_length=512)
    title: str | None = Field(default=None, max_length=4_096)
    description: str | None = Field(default=None, max_length=65_536)
    input_schema: JSON
    output_schema: JSON | None = None
    annotations: JSON | None = None

    @field_validator("name")
    @classmethod
    def _name_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("remote MCP tool name must not be blank")
        return value

    @model_validator(mode="after")
    def _schema_must_be_bounded(self) -> RemoteMcpToolSchema:
        _require_bounded_json_structure(self.input_schema, label="inputSchema")
        _require_finite_json(self.input_schema, label="inputSchema")
        _validate_json_schema(self.input_schema, label="inputSchema")
        if self.output_schema is not None:
            _require_bounded_json_structure(self.output_schema, label="outputSchema")
            _require_finite_json(self.output_schema, label="outputSchema")
            _validate_json_schema(self.output_schema, label="outputSchema")
        if self.annotations is not None:
            _require_bounded_json_structure(self.annotations, label="annotations")
            _require_finite_json(self.annotations, label="annotations")
        payload = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(payload) > MAX_REMOTE_MCP_TOOL_SCHEMA_BYTES:
            raise ValueError(
                f"remote MCP tool schema exceeds {MAX_REMOTE_MCP_TOOL_SCHEMA_BYTES} bytes"
            )
        return self


def is_remote_mcp_control_query(tool: RemoteMcpToolSchema) -> bool:
    """Return whether discovery explicitly classifies a tool as a safe query.

    MCP annotations are advisory server claims, so this predicate is only one
    input to admission. Callers must additionally bind the invocation to the
    registered route and exact discovered server artifact before assigning the
    reserved control-query class.
    """
    annotations = tool.annotations
    return bool(
        annotations is not None
        and annotations.get("readOnlyHint") is True
        and annotations.get("destructiveHint") is False
    )


class RemoteMcpDiscoveryProvenance(BaseModel):
    """Durable evidence associated with one cached remote discovery."""

    model_config = ConfigDict(extra="forbid")

    source: str = REMOTE_MCP_CACHE_SOURCE
    discovery_job_id: str
    artifact_id: str
    artifact_sha256: str
    protocol_version: str | None = None
    server_info: JSON = Field(default_factory=dict)
    server_artifact: JSON = Field(default_factory=dict)

    @field_validator("artifact_sha256")
    @classmethod
    def _artifact_digest_must_be_sha256(cls, value: str) -> str:
        normalized = value.strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError("remote MCP discovery artifact SHA-256 must be 64 hex characters")
        return normalized

    @model_validator(mode="after")
    def _provenance_must_be_bounded(self) -> RemoteMcpDiscoveryProvenance:
        _require_bounded_json_structure(self.server_info, label="server_info")
        _require_bounded_json_structure(self.server_artifact, label="server_artifact")
        payload = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(payload) > MAX_REMOTE_MCP_PROVENANCE_BYTES:
            raise ValueError(
                f"remote MCP provenance exceeds {MAX_REMOTE_MCP_PROVENANCE_BYTES} bytes"
            )
        return self


def resolve_remote_tool_title(title: str | None, annotations: JSON | None) -> str | None:
    """Resolve a discovered tool's display title across both MCP title eras.

    ``Tool.title`` (MCP 2025-06-18) wins when present. A server that has not
    adopted the newer field but still declared the pre-2025-06-18
    ``annotations.title`` (MCP 2025-03-26) has it read verbatim -- provided
    it is a non-blank string -- rather than dropped. No title anywhere
    resolves to ``None``; this never synthesizes a title from the tool name.

    Shared by every tools/list ingestion path that builds a
    :class:`RemoteMcpToolSchema` (clio-relay#164): :func:`_parse_remote_tool`
    below (live discovery -> schema cache -> catalog projection) and
    ``jarvis_mcp_validation_contract._remote_contract_tool`` (the JARVIS
    remote-contract digest check). Both are compared against the same
    pinned contract sha for the same live server, so an un-shared
    resolution would let the two paths silently disagree about that
    server's schema.

    Note the ``.strip()`` check here only governs whether
    ``RemoteMcpToolSchema.title`` itself gets set -- it does not reach the
    wire. FastMCP's own ``Tool.to_mcp_tool()`` independently falls back
    title -> ``annotations.title`` when serializing a listed tool, and that
    fallback does not strip, so a whitespace-only ``annotations.title`` can
    still surface as a tool's wire-visible title even when this function
    resolves ``None`` (clio-relay#164 repair round, defect 4). Annotations
    are always forwarded byte-for-byte regardless of what this function
    returns -- fixing that upstream FastMCP behavior is out of scope here.
    """
    if title is not None:
        return title
    if isinstance(annotations, dict):
        annotations_title = annotations.get("title")
        if isinstance(annotations_title, str) and annotations_title.strip():
            return annotations_title
    return None


def _parse_remote_tool(value: object) -> RemoteMcpToolSchema:
    if not isinstance(value, dict):
        raise ValueError("remote MCP tools/list entries must be objects")
    tool = cast(JSON, value)
    name = tool.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("remote MCP tool name must be a non-empty string")
    input_schema = tool.get("inputSchema")
    if not isinstance(input_schema, dict):
        raise ValueError(f"remote MCP tool {name} inputSchema must be an object")
    title = tool.get("title")
    description = tool.get("description")
    output_schema = tool.get("outputSchema")
    annotations = tool.get("annotations")
    if title is not None and not isinstance(title, str):
        raise ValueError(f"remote MCP tool {name} title must be a string")
    if description is not None and not isinstance(description, str):
        raise ValueError(f"remote MCP tool {name} description must be a string")
    if output_schema is not None and not isinstance(output_schema, dict):
        raise ValueError(f"remote MCP tool {name} outputSchema must be an object")
    if annotations is not None and not isinstance(annotations, dict):
        raise ValueError(f"remote MCP tool {name} annotations must be an object")
    resolved_title = resolve_remote_tool_title(title, cast(JSON | None, annotations))
    return RemoteMcpToolSchema(
        name=name,
        title=resolved_title,
        description=description,
        input_schema=cast(JSON, input_schema),
        output_schema=cast(JSON | None, output_schema),
        annotations=cast(JSON | None, annotations),
    )


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value.lower())
    )


def _server_artifact_verified(server_artifact: JSON) -> bool:
    return (
        server_artifact.get("verified") is True
        and server_artifact.get("server_process_artifact_verified") is True
        and isinstance(server_artifact.get("executable"), dict)
    )


def _immutable_remote_mcp_install_verified(server_artifact: JSON) -> bool:
    """Accept immutable wheel launches and wheel-backed persistent uv tools."""
    install_source = server_artifact.get("install_source")
    if install_source == "wheel":
        return True
    if install_source != "uv-tool":
        return False
    install_spec = server_artifact.get("install_spec")
    python_runtime = server_artifact.get("python_distribution_runtime")
    if (
        not isinstance(install_spec, str)
        or not install_spec.lower().endswith(".whl")
        or not isinstance(python_runtime, dict)
        or cast(JSON, python_runtime).get("runtime_closure_verified") is not True
    ):
        return False
    if server_artifact.get("nested_launcher") is not True:
        return True
    nested_runtime = server_artifact.get("nested_runtime")
    return (
        isinstance(nested_runtime, dict)
        and cast(JSON, nested_runtime).get("persistent_tool") is True
        and cast(JSON, nested_runtime).get("locked_runtime_verified") is True
    )


def _stable_digest(value: JSON) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()

"""Tests for the discovered remote MCP tool schema cluster (#231).

Two concerns:

1. **Extraction seam** -- ``clio_relay.remote_mcp_tool_schema`` is the owner
   module; ``clio_relay.remote_mcp`` must re-export ``RemoteMcpToolSchema``,
   ``RemoteMcpDiscoveryProvenance``, and ``is_remote_mcp_control_query``
   under an identical binding (proven by identity, not structural equality)
   so existing callers and tests keep resolving to the *same* object after
   the move.
2. **Identity/verification helper behavior** -- ``_is_sha256``,
   ``_server_artifact_verified``, ``_immutable_remote_mcp_install_verified``,
   and ``_stable_digest`` were previously exercised only indirectly, as
   internals of ``remote_mcp_server_artifact_binding_verified`` and
   ``cache_entry_from_discovery_artifact`` in ``tests/test_remote_mcp.py``
   (which correctly stay there -- they test those higher-level contracts).
   This file adds net-new focused coverage for the helpers themselves.
"""

from __future__ import annotations

from typing import Any

import pytest

import clio_relay.jarvis_mcp_validation_contract as jarvis_mcp_validation_contract
import clio_relay.remote_mcp as remote_mcp
import clio_relay.remote_mcp_tool_schema as remote_mcp_tool_schema
from clio_relay.remote_mcp import remote_mcp_schema_digest
from clio_relay.remote_mcp_tool_schema import (
    RemoteMcpDiscoveryProvenance,
    RemoteMcpToolSchema,
    _immutable_remote_mcp_install_verified,
    _is_sha256,
    _parse_remote_tool,  # pyright: ignore[reportPrivateUsage]
    _server_artifact_verified,
    _stable_digest,
    is_remote_mcp_control_query,
    resolve_remote_tool_title,
)

_INPUT_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}, "additionalProperties": False}


def test_remote_mcp_reexports_are_identical_objects() -> None:
    assert remote_mcp.RemoteMcpToolSchema is RemoteMcpToolSchema
    assert remote_mcp.RemoteMcpDiscoveryProvenance is RemoteMcpDiscoveryProvenance
    assert remote_mcp.is_remote_mcp_control_query is is_remote_mcp_control_query


def test_is_sha256_accepts_only_lowercase_64_char_hex() -> None:
    assert _is_sha256("a" * 64) is True
    assert _is_sha256("A" * 64) is True  # normalized via .lower() internally
    assert _is_sha256("a" * 63) is False
    assert _is_sha256("g" * 64) is False
    assert _is_sha256(None) is False
    assert _is_sha256(12345) is False


def test_server_artifact_verified_requires_all_three_fields() -> None:
    verified = {
        "verified": True,
        "server_process_artifact_verified": True,
        "executable": {"path": "/usr/bin/example"},
    }
    assert _server_artifact_verified(verified) is True
    assert _server_artifact_verified({**verified, "verified": False}) is False
    assert _server_artifact_verified({**verified, "executable": "not-a-dict"}) is False


def test_immutable_install_verified_accepts_wheel_source() -> None:
    assert _immutable_remote_mcp_install_verified({"install_source": "wheel"}) is True


def test_immutable_install_verified_rejects_unknown_source() -> None:
    assert _immutable_remote_mcp_install_verified({"install_source": "pip"}) is False


def test_immutable_install_verified_accepts_verified_uv_tool_wheel() -> None:
    artifact = {
        "install_source": "uv-tool",
        "install_spec": "package-1.0.0-py3-none-any.whl",
        "python_distribution_runtime": {"runtime_closure_verified": True},
    }
    assert _immutable_remote_mcp_install_verified(artifact) is True


def test_immutable_install_verified_rejects_uv_tool_without_verified_runtime() -> None:
    artifact = {
        "install_source": "uv-tool",
        "install_spec": "package-1.0.0-py3-none-any.whl",
        "python_distribution_runtime": {"runtime_closure_verified": False},
    }
    assert _immutable_remote_mcp_install_verified(artifact) is False


def test_immutable_install_verified_requires_persistent_locked_nested_runtime() -> None:
    base = {
        "install_source": "uv-tool",
        "install_spec": "package-1.0.0-py3-none-any.whl",
        "python_distribution_runtime": {"runtime_closure_verified": True},
        "nested_launcher": True,
    }
    assert _immutable_remote_mcp_install_verified(base) is False
    nested_runtime = {"persistent_tool": True, "locked_runtime_verified": True}
    assert (
        _immutable_remote_mcp_install_verified({**base, "nested_runtime": nested_runtime}) is True
    )


def test_stable_digest_is_order_independent_and_deterministic() -> None:
    first = _stable_digest({"a": 1, "b": 2})
    second = _stable_digest({"b": 2, "a": 1})
    assert first == second
    assert first == remote_mcp_tool_schema._stable_digest({"a": 1, "b": 2})  # noqa: SLF001
    assert first != _stable_digest({"a": 1, "b": 3})


# clio-relay#164: upstream tool titles must ride the discovery parser -- the
# projection later just forwards whatever RemoteMcpToolSchema.title resolves
# to, so the fallback precedence (Tool.title, then annotations.title, then
# absent) belongs here, at the one place raw tools/list JSON becomes typed.


def test_parse_remote_tool_forwards_top_level_title_verbatim() -> None:
    parsed = _parse_remote_tool(
        {
            "name": "inspect",
            "title": "Inspect Science Data",
            "inputSchema": _INPUT_SCHEMA,
        }
    )
    assert parsed.title == "Inspect Science Data"


def test_parse_remote_tool_top_level_title_takes_precedence_over_annotations() -> None:
    """MCP 2025-06-18's Tool.title wins when a server declares both."""
    parsed = _parse_remote_tool(
        {
            "name": "inspect",
            "title": "Inspect Science Data",
            "inputSchema": _INPUT_SCHEMA,
            "annotations": {"title": "Stale Annotation Title", "readOnlyHint": True},
        }
    )
    assert parsed.title == "Inspect Science Data"
    # The raw annotations object is preserved byte-for-byte, not rewritten.
    assert parsed.annotations == {"title": "Stale Annotation Title", "readOnlyHint": True}


def test_parse_remote_tool_falls_back_to_annotations_title_when_absent() -> None:
    """A pre-2025-06-18 server only declared the title in annotations.title."""
    parsed = _parse_remote_tool(
        {
            "name": "inspect",
            "inputSchema": _INPUT_SCHEMA,
            "annotations": {"title": "Inspect Science Data", "readOnlyHint": True},
        }
    )
    assert parsed.title == "Inspect Science Data"
    assert parsed.annotations == {"title": "Inspect Science Data", "readOnlyHint": True}


def test_parse_remote_tool_with_no_title_anywhere_stays_none_not_fabricated() -> None:
    """No synthesis: an upstream tool with no declared title projects none at all."""
    parsed = _parse_remote_tool({"name": "inspect", "inputSchema": _INPUT_SCHEMA})
    assert parsed.title is None

    with_annotations = _parse_remote_tool(
        {
            "name": "inspect",
            "inputSchema": _INPUT_SCHEMA,
            "annotations": {"readOnlyHint": True},
        }
    )
    assert with_annotations.title is None


def test_parse_remote_tool_ignores_malformed_annotations_title() -> None:
    """A non-string or blank annotations.title does not populate the SCHEMA's title.

    Scoped to ``RemoteMcpToolSchema.title`` only -- this is not a user-visible
    guarantee. ``annotations`` is forwarded byte-for-byte regardless of this
    resolution (unchanged, by design), and FastMCP's own
    ``Tool.to_mcp_tool()`` independently falls back title -> annotations.title
    at the wire layer without stripping. So a degenerate server that declares
    only a whitespace-only ``annotations.title`` still has that string appear
    as the tool's wire-visible title in ``tools/list`` -- the schema-level
    guard here is bypassed downstream by FastMCP's own fallback (clio-relay#164
    repair round, defect 4). Fixing that upstream FastMCP behavior is out of
    scope; this test only pins the schema layer, which is the layer clio-relay
    owns and the layer ``remote_mcp_schema_digest`` hashes.
    """
    blank = _parse_remote_tool(
        {
            "name": "inspect",
            "inputSchema": _INPUT_SCHEMA,
            "annotations": {"title": "   "},
        }
    )
    assert blank.title is None

    wrong_type = _parse_remote_tool(
        {
            "name": "inspect",
            "inputSchema": _INPUT_SCHEMA,
            "annotations": {"title": 42},
        }
    )
    assert wrong_type.title is None


# clio-relay#164 repair round, defect 2: `_parse_remote_tool` (discovery ->
# schema cache -> catalog) and `_remote_contract_tool` (JARVIS live
# remote-contract digest check) are two independent tools/list ingestion
# paths that both build a RemoteMcpToolSchema and both feed
# remote_mcp_schema_digest compared against the same pinned contract sha for
# the same live server. Before this fix, only the discovery path resolved
# annotations.title as a title fallback -- an annotations-only-title server
# would make the two paths disagree about that server's digest. Both now
# route through the shared resolve_remote_tool_title helper.


def test_resolve_remote_tool_title_precedence_and_absence() -> None:
    assert resolve_remote_tool_title("Explicit", {"title": "Stale"}) == "Explicit"
    assert resolve_remote_tool_title(None, {"title": "Annotated"}) == "Annotated"
    assert resolve_remote_tool_title(None, {"title": "   "}) is None
    assert resolve_remote_tool_title(None, {"title": 42}) is None
    assert resolve_remote_tool_title(None, None) is None
    assert resolve_remote_tool_title(None, {}) is None


@pytest.mark.parametrize(
    "raw_tool",
    [
        pytest.param(
            {
                "name": "inspect",
                "title": "Inspect Science Data",
                "inputSchema": _INPUT_SCHEMA,
            },
            id="top_level_title",
        ),
        pytest.param(
            {
                "name": "inspect",
                "inputSchema": _INPUT_SCHEMA,
                "annotations": {"title": "Inspect Science Data", "readOnlyHint": True},
            },
            id="annotations_only_title",
        ),
        pytest.param(
            {
                "name": "inspect",
                "inputSchema": _INPUT_SCHEMA,
            },
            id="no_title",
        ),
    ],
)
def test_parse_remote_tool_and_remote_contract_tool_agree_on_title_and_digest(
    raw_tool: dict[str, Any],
) -> None:
    """The two live tools/list ingestion paths must resolve one identical title.

    ``_parse_remote_tool`` (remote_mcp_tool_schema.py, this module) and
    ``_remote_contract_tool`` (jarvis_mcp_validation_contract.py) parse the
    exact same untrusted wire shape. If they disagreed on title, they would
    silently disagree on ``remote_mcp_schema_digest`` for the identical live
    server -- both are compared against the same pinned contract sha
    (``CLIO_KIT_JARVIS_USER_CONTRACT_SHA256`` / ``_BY_ID``).
    """
    from_discovery = _parse_remote_tool(raw_tool)
    from_contract = jarvis_mcp_validation_contract._remote_contract_tool(  # pyright: ignore[reportPrivateUsage]
        raw_tool
    )

    assert from_discovery.title == from_contract.title
    assert remote_mcp_schema_digest([from_discovery]) == remote_mcp_schema_digest([from_contract])

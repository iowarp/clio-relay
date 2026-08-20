"""Tests for the virtual remote MCP alias assignment cluster (#231).

None of these six functions were tested directly by name before this slice
-- every prior test exercised alias assignment only indirectly, through
``build_virtual_remote_mcp_catalog``'s end-to-end alias output in
``tests/test_remote_mcp.py`` (which correctly stays there -- it tests the
catalog-assembly contract, not these naming primitives). This file adds
net-new focused unit coverage for the primitives themselves.
"""

from __future__ import annotations

import pytest

from clio_relay.remote_mcp_aliasing import (
    MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH,
    _alias_with_suffix,
    _bounded_base_alias,
    _collision_alias,
    _profile_allows,
    _safe_name,
)


def test_bounded_base_alias_passes_through_short_names() -> None:
    assert _bounded_base_alias("remote_alpha_tool") == "remote_alpha_tool"


def test_bounded_base_alias_hashes_overlong_names() -> None:
    base = "remote_" + ("x" * 100)
    bounded = _bounded_base_alias(base)
    assert len(bounded) <= MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH
    # Deterministic: the same overlong base always bounds to the same alias.
    assert bounded == _bounded_base_alias(base)


def test_alias_with_suffix_keeps_a_readable_prefix() -> None:
    result = _alias_with_suffix("remote_science_tool", "abc123")
    assert result.endswith("_abc123")
    assert len(result) <= MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH


def test_alias_with_suffix_falls_back_when_head_is_empty() -> None:
    assert _alias_with_suffix("____", "abc123") == "remote_abc123"


def test_alias_with_suffix_rejects_suffix_leaving_no_prefix() -> None:
    with pytest.raises(ValueError, match="no readable prefix"):
        _alias_with_suffix("base", "x" * MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH)


def test_collision_alias_prefers_shortest_unique_identity_prefix() -> None:
    alias = _collision_alias("remote_tool", "clusterA-serverX", blocked=set())
    assert alias not in set()
    assert alias.startswith("remote_tool_")


def test_collision_alias_avoids_every_blocked_name() -> None:
    blocked = {f"remote_tool_{'clusterA-serverX'[:length]}" for length in range(10, 20)}
    alias = _collision_alias("remote_tool", "clusterA-serverX", blocked=blocked)
    assert alias not in blocked


def test_profile_allows_all_admits_every_declared_profile() -> None:
    assert _profile_allows([], "all") is True


def test_profile_allows_normalizes_blank_and_agent_to_user() -> None:
    assert _profile_allows(["user"], "") is True
    assert _profile_allows(["user"], "agent") is True
    assert _profile_allows(["operator"], "") is False


def test_profile_allows_requires_exact_declared_match_otherwise() -> None:
    assert _profile_allows(["operator"], "operator") is True
    assert _profile_allows(["operator"], "user") is False


def test_safe_name_normalizes_to_lowercase_ascii_identifier() -> None:
    assert _safe_name("Science Catalog!!") == "science_catalog"


def test_safe_name_falls_back_to_a_stable_hash_for_all_punctuation() -> None:
    result = _safe_name("!!!")
    assert result.startswith("unnamed_")
    assert result == _safe_name("!!!")

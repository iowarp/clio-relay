"""Tests for the remote MCP release-acceptance evidence wire models (#231).

Two concerns:

1. **Extraction seam** -- ``clio_relay.remote_mcp_acceptance_models`` is the
   owner module; ``clio_relay.remote_mcp`` must re-export every model class
   it still references directly under an identical binding (proven by
   identity, not structural equality) so existing callers -- ``cli.py``,
   remote_mcp.py's own validator functions, and tests/test_remote_mcp.py's
   ``remote_mcp.RemoteMcpAcceptanceCheck(...)`` construction sites -- keep
   resolving to the *same* class after the move.
2. **Path-canonicalization primitive behavior** -- ``_is_canonical_absolute_posix_path``
   and ``_is_canonical_relative_posix_path`` were previously exercised only
   indirectly, as internals of the Spack evidence models' own field/model
   validators in ``tests/test_remote_mcp.py`` (which correctly stay there --
   they test those higher-level contracts). This file adds net-new focused
   coverage for the two primitives themselves.
"""

from __future__ import annotations

import clio_relay.remote_mcp as remote_mcp
from clio_relay.remote_mcp_acceptance_models import (
    RemoteMcpAcceptanceReport,
    RemoteMcpCatalogIssue,
    RemoteMcpSpackConfigurationObservation,
    RemoteMcpSpackInstallTransitionEvidence,
    RemoteMcpSpackTransitionArtifactEvidence,
    RemoteMcpSpackTransitionCallEvidence,
    RemoteMcpSpackTransitionStdioEvidence,
    RemoteMcpStructuredResultExpectation,
    _is_canonical_absolute_posix_path,
    _is_canonical_relative_posix_path,
)


def test_remote_mcp_reexports_referenced_model_classes() -> None:
    assert remote_mcp.RemoteMcpCatalogIssue is RemoteMcpCatalogIssue
    assert remote_mcp.RemoteMcpStructuredResultExpectation is RemoteMcpStructuredResultExpectation
    assert (
        remote_mcp.RemoteMcpSpackTransitionArtifactEvidence
        is RemoteMcpSpackTransitionArtifactEvidence
    )
    assert remote_mcp.RemoteMcpSpackTransitionStdioEvidence is RemoteMcpSpackTransitionStdioEvidence
    assert (
        remote_mcp.RemoteMcpSpackConfigurationObservation is RemoteMcpSpackConfigurationObservation
    )
    assert remote_mcp.RemoteMcpSpackTransitionCallEvidence is RemoteMcpSpackTransitionCallEvidence
    assert (
        remote_mcp.RemoteMcpSpackInstallTransitionEvidence
        is RemoteMcpSpackInstallTransitionEvidence
    )
    assert remote_mcp.RemoteMcpAcceptanceReport is RemoteMcpAcceptanceReport


def test_is_canonical_absolute_posix_path_accepts_normalized_paths() -> None:
    assert _is_canonical_absolute_posix_path("/a/b/c") is True


def test_is_canonical_absolute_posix_path_rejects_relative_double_slash_and_traversal() -> None:
    assert _is_canonical_absolute_posix_path("relative/path") is False
    assert _is_canonical_absolute_posix_path("//a/b") is False
    assert _is_canonical_absolute_posix_path("/") is False
    assert _is_canonical_absolute_posix_path("/a/../b") is False
    assert _is_canonical_absolute_posix_path(None) is False
    assert _is_canonical_absolute_posix_path(123) is False


def test_is_canonical_absolute_posix_path_rejects_control_characters() -> None:
    assert _is_canonical_absolute_posix_path("/a\x00b") is False


def test_is_canonical_relative_posix_path_accepts_normalized_relative_paths() -> None:
    assert _is_canonical_relative_posix_path("a/b/c") is True


def test_is_canonical_relative_posix_path_rejects_absolute_empty_dot_and_traversal() -> None:
    assert _is_canonical_relative_posix_path("/absolute") is False
    assert _is_canonical_relative_posix_path("") is False
    assert _is_canonical_relative_posix_path(".") is False
    assert _is_canonical_relative_posix_path("a/../b") is False
    assert _is_canonical_relative_posix_path(None) is False

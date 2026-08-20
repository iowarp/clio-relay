"""Tests for the remote MCP schema discovery cache owner module (#231).

Two concerns:

1. **Extraction seam** -- ``clio_relay.remote_mcp_cache`` is the owner
   module; ``clio_relay.remote_mcp`` must re-export every one of its nine
   public names under an identical binding (proven by identity, not
   structural equality) so existing callers -- cli.py, mcp_server.py,
   jarvis_mcp.py, jarvis_mcp_validation.py, jarvis_service_runtime.py,
   endpoint.py, and this suite's own ``tests/test_remote_mcp.py`` -- keep
   resolving to the *same* object after the move.
2. **Digest/verification helper behavior** -- ``remote_mcp_execution_fingerprint``
   and ``remote_mcp_server_artifact_binding_verified`` were previously
   exercised only indirectly, as internals of
   ``cache_entry_from_discovery_artifact`` and route admission in
   ``tests/test_remote_mcp.py`` (which correctly stays there -- it tests
   those higher-level contracts). This file adds net-new focused coverage
   for the two helpers themselves. ``RemoteMcpSchemaCacheEntry``,
   ``RemoteMcpSchemaCache``, ``cache_entry_from_discovery_artifact``, and
   the remaining digest helpers already have extensive direct coverage in
   ``tests/test_remote_mcp.py`` and are not duplicated here.
"""

from __future__ import annotations

from datetime import UTC, datetime

import clio_relay.remote_mcp as remote_mcp
from clio_relay.cluster_config import RemoteMcpServerConfig
from clio_relay.remote_mcp_cache import (
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    cache_entry_from_discovery_artifact,
    default_remote_mcp_cache_path,
    remote_mcp_execution_fingerprint,
    remote_mcp_registration_revision,
    remote_mcp_schema_digest,
    remote_mcp_server_artifact_binding_verified,
    remote_mcp_server_artifact_digest,
)


def test_remote_mcp_reexports_are_identical_objects() -> None:
    assert remote_mcp.RemoteMcpSchemaCacheEntry is RemoteMcpSchemaCacheEntry
    assert remote_mcp.RemoteMcpSchemaCache is RemoteMcpSchemaCache
    assert remote_mcp.default_remote_mcp_cache_path is default_remote_mcp_cache_path
    assert remote_mcp.remote_mcp_execution_fingerprint is remote_mcp_execution_fingerprint
    assert remote_mcp.remote_mcp_registration_revision is remote_mcp_registration_revision
    assert remote_mcp.remote_mcp_schema_digest is remote_mcp_schema_digest
    assert remote_mcp.remote_mcp_server_artifact_digest is remote_mcp_server_artifact_digest
    assert (
        remote_mcp.remote_mcp_server_artifact_binding_verified
        is remote_mcp_server_artifact_binding_verified
    )
    assert remote_mcp.cache_entry_from_discovery_artifact is cache_entry_from_discovery_artifact


def _registration(**overrides: object) -> RemoteMcpServerConfig:
    defaults: dict[str, object] = {
        "command": "science-server",
        "args": ["--port", "7000"],
        "env_from": {},
    }
    defaults.update(overrides)
    return RemoteMcpServerConfig.model_validate(defaults)


def test_execution_fingerprint_depends_only_on_command_args_env() -> None:
    base = _registration()
    same_execution = _registration()
    assert remote_mcp_execution_fingerprint(base) == remote_mcp_execution_fingerprint(
        same_execution
    )


def test_execution_fingerprint_changes_with_args() -> None:
    base = _registration()
    changed = _registration(args=["--port", "7001"])
    assert remote_mcp_execution_fingerprint(base) != remote_mcp_execution_fingerprint(changed)


def test_server_artifact_binding_verified_requires_matching_digest() -> None:
    server_artifact = {
        "verified": True,
        "server_process_artifact_verified": True,
        "executable": {"path": "/usr/bin/example"},
        "install_source": "wheel",
        "install_artifact_sha256": "a" * 64,
    }
    expected_digest = remote_mcp_server_artifact_digest(server_artifact)
    assert (
        remote_mcp_server_artifact_binding_verified(
            server_artifact, expected_digest=expected_digest
        )
        is True
    )


def test_server_artifact_binding_verified_rejects_digest_mismatch() -> None:
    server_artifact = {
        "verified": True,
        "server_process_artifact_verified": True,
        "executable": {"path": "/usr/bin/example"},
        "install_source": "wheel",
        "install_artifact_sha256": "a" * 64,
    }
    assert (
        remote_mcp_server_artifact_binding_verified(server_artifact, expected_digest="b" * 64)
        is False
    )


def test_server_artifact_binding_verified_rejects_malformed_expected_digest() -> None:
    assert remote_mcp_server_artifact_binding_verified({}, expected_digest="not-a-digest") is False
    assert remote_mcp_server_artifact_binding_verified({}, expected_digest=None) is False


def test_schema_cache_entry_freshness_boundary() -> None:
    tools: list[object] = []
    entry = RemoteMcpSchemaCacheEntry(
        cluster="alpha",
        server_name="science",
        execution_fingerprint="fingerprint",
        discovered_at=datetime(2026, 1, 1, tzinfo=UTC),
        expires_at=datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
        schema_digest=remote_mcp_schema_digest(tools),
        tools=tools,
        provenance={
            "discovery_job_id": "job-1",
            "artifact_id": "artifact-1",
            "artifact_sha256": "a" * 64,
        },
    )
    assert entry.is_fresh(now=datetime(2026, 1, 1, 0, 1, tzinfo=UTC)) is True
    assert entry.is_fresh(now=datetime(2026, 1, 1, 0, 10, tzinfo=UTC)) is False

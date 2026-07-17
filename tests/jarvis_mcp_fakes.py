"""Exact built-in JARVIS MCP artifact evidence for trust-boundary tests."""

from __future__ import annotations

from typing import Any

from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    jarvis_cd_lock_binding_expectation,
)


def verified_jarvis_server_artifact() -> dict[str, Any]:
    """Return minimal evidence satisfying the exact outer and nested release pins."""
    expected = jarvis_cd_lock_binding_expectation()
    return {
        "verified": True,
        "server_process_artifact_verified": True,
        "install_source": "uv-tool",
        "install_artifact_sha256": CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        "executable": {
            "path": "/home/operator/.local/bin/clio-kit",
            "sha256": "a" * 64,
        },
        "python_distribution_runtime": {
            "distribution": "clio-kit",
            "distribution_version": CLIO_KIT_JARVIS_MCP_VERSION,
            "entry_point": "clio-kit",
            "runtime_closure_verified": True,
        },
        "nested_runtime": {
            "schema_version": "clio-kit.locked-server.v4",
            "server_name": "jarvis",
            "persistent_tool": True,
            "locked_runtime_verified": True,
            "jarvis_cd_lock_binding": {
                "schema_version": "clio-relay.jarvis-cd-lock-binding.v1",
                "dependency": "jarvis-cd",
                "verified": True,
                "error": None,
                "expected_version": expected["version"],
                "expected_url": expected["url"],
                "expected_sha256": expected["sha256"],
                "observed_version": expected["version"],
                "observed_source_url": expected["url"],
                "observed_wheel_url": expected["url"],
                "observed_wheel_sha256": expected["sha256"],
                "jarvis_mcp_package_entry_count": 1,
                "resolved_dependency_entry_count": 1,
                "observed_resolved_dependency_entries": [{"name": "jarvis-cd"}],
                "metadata_requirement_entry_count": 1,
                "observed_metadata_requirement_entries": [
                    {"name": "jarvis-cd", "url": expected["url"]}
                ],
                "observed_metadata_requirement_urls": [expected["url"]],
                "package_entry_count": 1,
                "wheel_entry_count": 1,
            },
        },
    }

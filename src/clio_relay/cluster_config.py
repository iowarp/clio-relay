"""Local cluster registry for relay targets.

This module is a thin facade (iowarp/clio-relay#231 split/cluster-config):
the real implementation moved to owner modules below so every existing
`from clio_relay.cluster_config import X` import across the codebase keeps
resolving unchanged. Only two genuinely small, self-contained pieces stay
resident here (`CLUSTER_REGISTRY_ENV` and `default_registry_path`), plus
`import os` -- which `default_registry_path` needs for real, and which also
happens to be what `tests/test_cluster_config.py` patches directly via
`monkeypatch.setattr(cluster_config.os, ...)` (safe regardless of relocation:
`cluster_config.os` and `<owner module>.os` are the exact same stdlib module
object, so a patch through either reaches every caller). `import time` has no
functional use here -- it stays only so that same test file's
`monkeypatch.setattr(cluster_config.time, "sleep", ...)` calls keep resolving
through the same singleton-module mechanism.

Owner modules:
    cluster_config_models          -- the Pydantic schema (DirectTransportConfig
                                       through ClusterDefinition)
    cluster_config_registry        -- ClusterRegistry + cluster_route_revision
    cluster_config_io              -- bounded, integrity-checked configuration
                                       file reads
    cluster_config_windows_primitives -- leaf Win32 ctypes/SID primitives
    cluster_config_windows_acl     -- reading back and verifying a Windows ACL
    cluster_config_windows_paths   -- creating/enforcing owner-private
                                       Windows files and directories
    cluster_config_windows_guard   -- the parent-rename guard and opening an
                                       existing file under an enforced ACL
"""

from __future__ import annotations

import os
import time  # noqa: F401 -- see module docstring: kept for cluster_config.time patch compatibility
from pathlib import Path

from clio_relay.cluster_config_io import (
    CONFIG_READ_RETRY_SECONDS,
    MAX_CLUSTER_REGISTRY_BYTES,
    MAX_CONFIG_READ_ATTEMPTS,
    read_bounded_configuration_bytes,
)
from clio_relay.cluster_config_models import (
    MAX_REMOTE_MCP_ALLOW_TOOLS,
    MAX_REMOTE_MCP_ARGS,
    MAX_REMOTE_MCP_ARGUMENT_BYTES,
    MAX_REMOTE_MCP_ENV_REFS,
    MAX_REMOTE_MCP_SCHEMA_CACHE_TTL_SECONDS,
    MAX_REMOTE_MCP_SERVERS_PER_CLUSTER,
    ClusterDefinition,
    ClusterTargetIdentity,
    DirectTransportConfig,
    FrpTransportConfig,
    IdentityAnchor,
    LiveTestConfig,
    RemoteMcpContract,
    RemoteMcpProfile,
    RemoteMcpServerConfig,
    WorkerCapacityPolicy,
)
from clio_relay.cluster_config_registry import (
    CONFIG_REPLACE_ATTEMPTS,
    MAX_CONFIGURED_CLUSTERS,
    MAX_REMOTE_MCP_REGISTRATIONS,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.cluster_config_windows_guard import (
    acquire_private_configuration_windows_parent_guard,
    open_private_configuration_windows_descriptor,
    release_private_configuration_windows_parent_guard,
)
from clio_relay.cluster_config_windows_paths import (
    create_private_configuration_directory,
    ensure_private_configuration_directory,
    ensure_private_configuration_path,
    ensure_private_configuration_windows_handle,
    open_private_atomic_file,
)

__all__ = [
    "CLUSTER_REGISTRY_ENV",
    "CONFIG_READ_RETRY_SECONDS",
    "CONFIG_REPLACE_ATTEMPTS",
    "MAX_CLUSTER_REGISTRY_BYTES",
    "MAX_CONFIG_READ_ATTEMPTS",
    "MAX_CONFIGURED_CLUSTERS",
    "MAX_REMOTE_MCP_ALLOW_TOOLS",
    "MAX_REMOTE_MCP_ARGS",
    "MAX_REMOTE_MCP_ARGUMENT_BYTES",
    "MAX_REMOTE_MCP_ENV_REFS",
    "MAX_REMOTE_MCP_REGISTRATIONS",
    "MAX_REMOTE_MCP_SCHEMA_CACHE_TTL_SECONDS",
    "MAX_REMOTE_MCP_SERVERS_PER_CLUSTER",
    "ClusterDefinition",
    "ClusterRegistry",
    "ClusterTargetIdentity",
    "DirectTransportConfig",
    "FrpTransportConfig",
    "IdentityAnchor",
    "LiveTestConfig",
    "RemoteMcpContract",
    "RemoteMcpProfile",
    "RemoteMcpServerConfig",
    "WorkerCapacityPolicy",
    "acquire_private_configuration_windows_parent_guard",
    "cluster_route_revision",
    "create_private_configuration_directory",
    "default_registry_path",
    "ensure_private_configuration_directory",
    "ensure_private_configuration_path",
    "ensure_private_configuration_windows_handle",
    "open_private_atomic_file",
    "open_private_configuration_windows_descriptor",
    "read_bounded_configuration_bytes",
    "release_private_configuration_windows_parent_guard",
]

CLUSTER_REGISTRY_ENV = "CLIO_RELAY_CLUSTER_REGISTRY"


def default_registry_path() -> Path:
    """Return the default local cluster registry path."""
    configured = os.getenv(CLUSTER_REGISTRY_ENV)
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(".clio-relay/clusters.json").resolve()

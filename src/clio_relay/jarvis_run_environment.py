"""Site runtime identity composed by the relay for one JARVIS MCP child.

The relay spawns the JARVIS MCP server itself, so it — not the operator's per
server registration — composes that child's environment. A cluster registers one
``spack_executable``; that path is the single Spack identity for every route on
the cluster that needs Spack. The Spack route already receives it through its
registered ``--spack-command`` argument. This module carries the same registered
path to the JARVIS run environment, where ``spack load`` runs inside the JARVIS
MCP child.

The JARVIS MCP server resolves Spack from ``JARVIS_MCP_SPACK_COMMAND`` first and
only then searches ``PATH``, ``SPACK_ROOT/bin``, ``~/.local/spack`` and
``/opt/spack``, so an explicitly composed variable is the typed mechanism rather
than an ambient search hit. The worker publishes the registered path under a
relay-owned name; the bundled MCP runner maps that name onto the child variable
the JARVIS server reads.
"""

from __future__ import annotations

import os
from pathlib import Path

from clio_relay.cluster_config import ClusterRegistry, default_registry_path
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.remote_values import expand_remote_value_on_host

RELAY_JARVIS_SPACK_COMMAND_ENV = "CLIO_RELAY_JARVIS_SPACK_COMMAND"
"""Relay-owned worker variable carrying one cluster's registered Spack path."""

JARVIS_MCP_SPACK_COMMAND_CHILD_ENV = "JARVIS_MCP_SPACK_COMMAND"
"""Variable the JARVIS MCP server reads before any filesystem search."""


def registered_site_spack_command(
    cluster: str,
    *,
    registry_path: Path | None = None,
) -> str | None:
    """Return the Spack executable one cluster registered, expanded for this host.

    Args:
        cluster: Name of the cluster whose registration is being executed.
        registry_path: Explicit registry location; defaults to the configured
            cluster registry of the running endpoint.

    Returns:
        The registered absolute path, or ``None`` when no registry is configured
        on this host, it does not describe this cluster, or the cluster declares
        no Spack executable. An absent declaration never invents a default.

    Raises:
        ConfigurationError: If a configured registry cannot be read, or the
            declared value cannot be expanded on this host. A registry that
            exists but cannot be understood is a refusal, never a quiet
            downgrade to an unresolved Spack runtime.
    """
    resolved_path = registry_path or default_registry_path()
    if not resolved_path.exists():
        return None
    try:
        registry = ClusterRegistry.load(resolved_path)
    except (OSError, ValueError, RelayError) as error:
        raise ConfigurationError(
            f"configured cluster registry could not be read for {cluster}: {error}"
        ) from error
    definition = registry.clusters.get(cluster)
    if definition is None or definition.spack_executable is None:
        return None
    return expand_remote_value_on_host(
        definition.spack_executable,
        field="spack_executable",
        home=os.path.expanduser("~"),
    )


def jarvis_run_environment_values(spack_command: str | None) -> dict[str, str]:
    """Return the relay-composed run-environment values for a JARVIS MCP call.

    Args:
        spack_command: The cluster's registered Spack executable, or ``None``
            when the cluster declares none.

    Returns:
        A mapping to merge into the MCP runner environment. It is empty when the
        cluster declares no Spack executable, leaving behavior unchanged.
    """
    if spack_command is None:
        return {}
    return {RELAY_JARVIS_SPACK_COMMAND_ENV: spack_command}

"""Scope a host-local ``session teardown`` to its cluster's registered core.

iowarp/clio-relay#203: for a cluster whose relay runs on this host
(``remote_cli.should_execute_on_cluster`` is false), teardown opens the queue,
storage, and worker-lifetime state through ``RelaySettings.from_env()``. That
reads ``CLIO_RELAY_CORE_DIR``/``CLIO_RELAY_SPOOL_DIR`` and falls back to the
default core when they are unset; it never consults the registry entry the
operator named with ``--cluster``. On a shared login node the default core can
belong to a different deployment.

This check runs before teardown touches any local state and refuses unless the
resolved paths are exactly the registry entry's. It never retargets: an operator
who forgot the environment gets a refusal naming both paths, not a teardown of
whichever core the defaults pick.
"""

from __future__ import annotations

from pathlib import Path

from clio_relay.cluster_config_models import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.errors import TeardownCoreScopeError
from clio_relay.remote_values import expand_remote_value_on_host


def registered_local_path(value: str, *, field: str) -> Path:
    """Resolve a registry ``core_dir``/``spool_dir`` value on this host."""
    expanded = expand_remote_value_on_host(value, field=field, home=str(Path.home()))
    return Path(expanded).resolve()


def require_registered_local_core(definition: ClusterDefinition, settings: RelaySettings) -> None:
    """Refuse unless ``settings`` name the cluster's registered core and spool.

    Raises:
        TeardownCoreScopeError: If either resolved path differs from the
            registry entry's path after ``$HOME`` expansion and symlink
            resolution.
    """
    for field, registered_value, resolved_value in (
        ("core_dir", definition.core_dir, settings.core_dir),
        ("spool_dir", definition.spool_dir, settings.spool_dir),
    ):
        registered = registered_local_path(registered_value, field=field)
        resolved = Path(resolved_value).expanduser().resolve()
        if registered != resolved:
            raise TeardownCoreScopeError(
                cluster=definition.name,
                field=field,
                registered=str(registered),
                resolved=str(resolved),
            )

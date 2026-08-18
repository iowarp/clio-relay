"""Reconcile a cluster's runtime pin with what ``cluster bootstrap`` produced.

Bootstrap installs the relay and publishes two stable, generation-independent
entry points: a launcher symlink and an install receipt, both under the
operator's home. The cluster registry carries its own
``relay_executable``/``relay_install_receipt`` pin, which every session and
worker command dereferences.

Nothing used to reconcile the two. An operator (or an earlier bootstrap) could
pin a cluster to a *generation-specific* path; when that generation was later
garbage-collected the pin was left dangling, and because bootstrap's preflight
probes the canonical launcher rather than the pin, a re-run happily reported
``noop_verified`` while every session command kept dying on the stale pointer
(clio-relay#158). An install that leaves a dead pointer behind is a silent
half-deploy, so bootstrap now re-points the registry at what it actually
produced and says so.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from clio_relay.cluster_config import ClusterRegistry

BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE = "$HOME/.local/bin/clio-relay"
"""Stable launcher published by bootstrap (``uv tool dir --bin``/clio-relay).

Republished on every generation activation, so it is the only relay path that
stays valid across upgrades and generation garbage collection.
"""

BOOTSTRAP_PRODUCED_INSTALL_RECEIPT = "$HOME/.local/share/clio-relay/install-receipt.json"
"""Stable install receipt published alongside the launcher."""

PIN_RECONCILIATION_SCHEMA = "clio-relay.bootstrap-pin-reconciliation.v1"


def pin_reconciliation_lines(record: dict[str, object]) -> list[str]:
    """Render the operator-visible report for a pin reconciliation.

    Empty when nothing changed, so an idempotent bootstrap stays quiet; a
    repoint is always announced rather than being applied silently.
    """
    if not record["changed"]:
        return []
    executable = cast(dict[str, object], record["relay_executable"])
    receipt = cast(dict[str, object], record["relay_install_receipt"])
    lines = [f"relay_executable_repointed={executable['after']}"]
    if receipt["before"] != receipt["after"]:
        lines.append(f"relay_install_receipt_repointed={receipt['after']}")
    return lines


def reconcile_cluster_runtime_pin(
    *,
    cluster: str,
    registry_path: Path,
) -> dict[str, object]:
    """Point one cluster's runtime pin at the paths bootstrap publishes.

    Returns a typed record describing what changed. ``changed`` is ``False``
    when the pin already matched, which keeps a repeated bootstrap idempotent
    and silent.

    Only ``relay_executable`` and ``relay_install_receipt`` are touched --
    every other field on the entry is preserved exactly, matching the
    ``cluster pin-runtime`` contract (clio-relay#205).

    The receipt is re-pointed only when the cluster ALREADY pins one. An
    absent receipt pin means the operator asked for no pinned-receipt
    verification, and quietly introducing one here would strengthen
    session-start verification behind their back -- repair the pin that
    exists, never mint a new one.
    """
    before: dict[str, object] = {}

    def update(registry: ClusterRegistry) -> None:
        definition = registry.require(cluster)
        before["relay_executable"] = definition.relay_executable
        before["relay_install_receipt"] = definition.relay_install_receipt
        definition.relay_executable = BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE
        if definition.relay_install_receipt is not None:
            definition.relay_install_receipt = BOOTSTRAP_PRODUCED_INSTALL_RECEIPT

    registry = ClusterRegistry.mutate(registry_path, update)
    definition = registry.require(cluster)
    previous_executable = cast(str, before.get("relay_executable"))
    previous_receipt = cast("str | None", before.get("relay_install_receipt"))
    changed = (
        previous_executable != definition.relay_executable
        or previous_receipt != definition.relay_install_receipt
    )
    return {
        "schema_version": PIN_RECONCILIATION_SCHEMA,
        "cluster": cluster,
        "changed": changed,
        "relay_executable": {
            "before": previous_executable,
            "after": definition.relay_executable,
        },
        "relay_install_receipt": {
            "before": previous_receipt,
            "after": definition.relay_install_receipt,
        },
    }

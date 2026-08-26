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

    Empty only when the pin already named the produced runtime, so an
    idempotent bootstrap stays quiet. Both a repoint AND a preserved custom pin
    are announced: each is a real deployment fact the operator must be able to
    see in the bootstrap output.
    """
    executable = cast(dict[str, object], record["relay_executable"])
    if record["action"] == "preserved_custom_pin":
        return [f"relay_executable_pin_preserved={executable['before']}"]
    if not record["changed"]:
        return []
    receipt = cast(dict[str, object], record["relay_install_receipt"])
    lines = [f"relay_executable_repointed={executable['after']}"]
    if receipt["before"] != receipt["after"]:
        lines.append(f"relay_install_receipt_repointed={receipt['after']}")
    return lines


def reconcile_cluster_runtime_pin(
    *,
    cluster: str,
    registry_path: Path,
    pinned_runtime_present: bool,
) -> dict[str, object]:
    """Repair one cluster's runtime pin, but only when it is proven broken.

    ``pinned_runtime_present`` is the caller's REMOTE observation of whether
    the currently pinned executable exists on the host. It is required, not
    inferred: ``relay_executable`` is a free-form path field, and an operator
    may legitimately point a cluster at a relay outside the canonical location
    -- a shared-filesystem deployment, or a second tenant on one host.
    Rewriting such a pin because it merely LOOKS unusual would silently
    relocate that cluster on a routine bootstrap re-run, so only a pin proven
    absent is repaired (clio-relay#158, review F1).

    Returns a typed record whose ``action`` is one of:

    ``unchanged``
        The pin already names the produced runtime.
    ``preserved_custom_pin``
        The pin differs from the produced runtime but RESOLVES on the host, so
        it is deliberate and is kept -- announced, never silent.
    ``repointed``
        The pin does not resolve on the host and has been repaired.

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
        # Classified inside the mutation so the whole reconciliation is ONE
        # registry read-modify-write. Loading first to inspect the pin would
        # double the config-directory traversal that every registry access
        # performs.
        definition = registry.require(cluster)
        before["relay_executable"] = definition.relay_executable
        before["relay_install_receipt"] = definition.relay_install_receipt
        if definition.relay_executable == BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE:
            before["action"] = "unchanged"
            return
        if pinned_runtime_present:
            before["action"] = "preserved_custom_pin"
            return
        before["action"] = "repointed"
        definition.relay_executable = BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE
        if definition.relay_install_receipt is not None:
            definition.relay_install_receipt = BOOTSTRAP_PRODUCED_INSTALL_RECEIPT

    registry = ClusterRegistry.mutate(registry_path, update)
    action = cast(str, before["action"])
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
        "action": action,
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


TARGET_IDENTITY_PIN_SCHEMA = "clio-relay.bootstrap-target-identity-pin.v1"


def pin_cluster_target_identity_from_one_pass_observation(
    *,
    cluster: str,
    registry_path: Path,
    observed_hostnames: list[str],
    observed_site_marker_sha256: str | None,
    ssh_host_key_sha256: list[str],
) -> dict[str, object]:
    """Pin a physical target identity a cold one-pass bootstrap just observed.

    clio-relay#209: closes the ``cluster pin-target`` manual-entry gap. Only
    fills a MISSING pin -- an operator-pinned identity that already exists is
    never overwritten by an in-session observation (the same
    proven-before-repaired discipline ``reconcile_cluster_runtime_pin``
    documents: an existing value is deliberate until proven otherwise, not
    merely different). When a pin already exists this is a no-op; the
    caller's existing ``cli_remote_worker_probe._remote_target_identity``
    verification-only dial still runs for that case.

    ``ssh_host_key_sha256`` must already be resolved by the caller from
    locally cached host keys (``ssh-keyscan``/``ssh-keygen`` against
    ``~/.ssh/known_hosts`` -- never authenticates against the target, so it
    never costs a dial); this function performs no I/O beyond the registry
    mutation.
    """
    from clio_relay.cluster_config import ClusterRegistry, ClusterTargetIdentity

    before: dict[str, object] = {}

    def update(registry: ClusterRegistry) -> None:
        definition = registry.require(cluster)
        if definition.target_identity is not None:
            before["action"] = "unchanged"
            return
        before["action"] = "pinned"
        definition.target_identity = ClusterTargetIdentity(
            hostnames=observed_hostnames,
            ssh_host_key_sha256=ssh_host_key_sha256,
            scheduler_cluster_name=None,
            site_marker_sha256=observed_site_marker_sha256,
        )

    registry = ClusterRegistry.mutate(registry_path, update)
    definition = registry.require(cluster)
    return {
        "schema_version": TARGET_IDENTITY_PIN_SCHEMA,
        "cluster": cluster,
        "action": before["action"],
        "target_identity": (
            definition.target_identity.model_dump(mode="json")
            if definition.target_identity is not None
            else None
        ),
    }

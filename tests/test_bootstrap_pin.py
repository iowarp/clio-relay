"""Unit tests for the bootstrap runtime-pin reconciliation (clio-relay#158)."""

from __future__ import annotations

from pathlib import Path

from clio_relay.bootstrap_pin import (
    BOOTSTRAP_PRODUCED_INSTALL_RECEIPT,
    BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE,
    PIN_RECONCILIATION_SCHEMA,
    pin_reconciliation_lines,
    reconcile_cluster_runtime_pin,
)
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry


def _registry(tmp_path: Path, **overrides: object) -> Path:
    path = tmp_path / "clusters.json"
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="ares",
                **overrides,  # pyright: ignore[reportArgumentType]
            )
        }
    ).save(path)
    return path


def test_a_valid_custom_pin_is_preserved_not_clobbered(tmp_path: Path) -> None:
    """A deliberately custom pin that RESOLVES on the host must survive bootstrap.

    Review F1: relay_executable is a free-form path field, and operators
    legitimately point a cluster at a relay outside $HOME -- a shared-filesystem
    deployment, or a second tenant on one host. Repointing unconditionally means
    a routine bootstrap re-run silently relocates that cluster onto the
    canonical launcher. Only a pin PROVEN ABSENT on the host may be rewritten.
    """
    path = _registry(
        tmp_path,
        relay_executable="/mnt/common/tenant-b/bin/clio-relay",
        relay_install_receipt="/mnt/common/tenant-b/install-receipt.json",
    )

    record = reconcile_cluster_runtime_pin(
        cluster="ares",
        registry_path=path,
        pinned_runtime_present=True,
    )

    assert record["changed"] is False
    assert record["action"] == "preserved_custom_pin"
    saved = ClusterRegistry.load(path).require("ares")
    assert saved.relay_executable == "/mnt/common/tenant-b/bin/clio-relay"
    assert saved.relay_install_receipt == "/mnt/common/tenant-b/install-receipt.json"
    # Preserving must be LOUD -- a silent skip hides a real deployment decision.
    assert pin_reconciliation_lines(record) == [
        "relay_executable_pin_preserved=/mnt/common/tenant-b/bin/clio-relay"
    ]


def test_a_dead_generation_pin_is_repointed_at_the_produced_runtime(tmp_path: Path) -> None:
    path = _registry(
        tmp_path,
        relay_executable="/srv/generations/gone/bin/clio-relay",
        relay_install_receipt="/srv/generations/gone/install-receipt.json",
    )

    record = reconcile_cluster_runtime_pin(
        cluster="ares", registry_path=path, pinned_runtime_present=False
    )

    assert record["schema_version"] == PIN_RECONCILIATION_SCHEMA
    assert record["changed"] is True
    saved = ClusterRegistry.load(path).require("ares")
    assert saved.relay_executable == BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE
    assert saved.relay_install_receipt == BOOTSTRAP_PRODUCED_INSTALL_RECEIPT


def test_reconciliation_is_idempotent(tmp_path: Path) -> None:
    path = _registry(
        tmp_path,
        relay_executable="/srv/generations/gone/bin/clio-relay",
        relay_install_receipt="/srv/generations/gone/install-receipt.json",
    )

    reconcile_cluster_runtime_pin(cluster="ares", registry_path=path, pinned_runtime_present=False)
    second = reconcile_cluster_runtime_pin(
        cluster="ares", registry_path=path, pinned_runtime_present=False
    )

    assert second["changed"] is False
    assert pin_reconciliation_lines(second) == []


def test_an_absent_receipt_pin_is_never_newly_minted(tmp_path: Path) -> None:
    """No pinned receipt means the operator asked for no pinned-receipt check.

    Quietly introducing one would strengthen session-start verification behind
    their back; repair the pin that exists, never mint a new one.
    """
    path = _registry(tmp_path, relay_executable="/srv/generations/gone/bin/clio-relay")

    record = reconcile_cluster_runtime_pin(
        cluster="ares", registry_path=path, pinned_runtime_present=False
    )

    saved = ClusterRegistry.load(path).require("ares")
    assert saved.relay_executable == BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE
    assert saved.relay_install_receipt is None
    assert pin_reconciliation_lines(record) == [
        f"relay_executable_repointed={BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE}"
    ]


def test_every_other_field_on_the_entry_is_preserved(tmp_path: Path) -> None:
    """Reconciling the pin must not disturb the rest of the cluster entry."""
    path = _registry(
        tmp_path,
        relay_executable="/srv/generations/gone/bin/clio-relay",
        core_dir="/scratch/relay-core",
        spool_dir="/scratch/relay-spool",
        scheduler_provider="slurm",
        dev_mode=True,
    )

    reconcile_cluster_runtime_pin(cluster="ares", registry_path=path, pinned_runtime_present=False)

    saved = ClusterRegistry.load(path).require("ares")
    assert saved.core_dir == "/scratch/relay-core"
    assert saved.spool_dir == "/scratch/relay-spool"
    assert saved.scheduler_provider == "slurm"
    assert saved.dev_mode is True


def test_a_repoint_is_always_announced(tmp_path: Path) -> None:
    """A silent repoint would hide a deployment change from the operator."""
    path = _registry(
        tmp_path,
        relay_executable="/srv/generations/gone/bin/clio-relay",
        relay_install_receipt="/srv/generations/gone/install-receipt.json",
    )

    lines = pin_reconciliation_lines(
        reconcile_cluster_runtime_pin(
            cluster="ares", registry_path=path, pinned_runtime_present=False
        )
    )

    assert lines == [
        f"relay_executable_repointed={BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE}",
        f"relay_install_receipt_repointed={BOOTSTRAP_PRODUCED_INSTALL_RECEIPT}",
    ]

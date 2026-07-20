"""Focused tests for machine-readable bootstrap reuse acceptance."""

from __future__ import annotations

from copy import deepcopy

import pytest

from clio_relay.bootstrap_acceptance import bootstrap_reuse_acceptance_evidence
from clio_relay.errors import RelayError


def _receipt(outcome: str = "noop_verified") -> dict[str, object]:
    service_start_count = 1 if outcome == "repaired" else 0
    return {
        "outcome": outcome,
        "operations": {
            "download_count": 0,
            "payload_transfer_count": 0,
            "payload_transfer_bytes": 0,
            "scheduler_submission_count": 0,
            "scheduler_cancellation_count": 0,
            "generation_gc_count": 0,
            "service_start_count": service_start_count,
            "service_restart_count": 0,
            "service_stop_count": 0,
            "service_enable_count": 0,
        },
        "jarvis_commands": {"count": 0, "argv": []},
        "jarvis_initialization": {"action": "preserved"},
        "jarvis_resource_graph": {"action": "preserved"},
        "jarvis_preservation": {
            "config_byte_identical": True,
            "resource_graph_byte_identical": True,
        },
        "components": {
            "clio-relay": {"action": "reused"},
            "clio-kit": {"action": "reused"},
            "jarvis-cd": {"action": "reused"},
        },
        "service": {"active_after": True, "enabled_after": True},
    }


@pytest.mark.parametrize(
    ("outcome", "elapsed_seconds", "maximum_seconds"),
    (("noop_verified", 30.0, 30.0), ("repaired", 60.0, 60.0)),
)
def test_reuse_acceptance_records_public_deadline_and_zero_mutation(
    outcome: str,
    elapsed_seconds: float,
    maximum_seconds: float,
) -> None:
    evidence = bootstrap_reuse_acceptance_evidence(
        _receipt(outcome),
        elapsed_seconds=elapsed_seconds,
    )

    assert evidence is not None
    assert evidence["outcome"] == outcome
    assert evidence["elapsed_seconds"] == elapsed_seconds
    assert evidence["maximum_seconds"] == maximum_seconds
    assert evidence["payload_free"] is True
    assert evidence["scheduler_untouched"] is True
    assert evidence["jarvis_preserved"] is True


def test_non_reuse_bootstrap_is_outside_the_fast_path_gate() -> None:
    assert (
        bootstrap_reuse_acceptance_evidence(
            {"outcome": "full"},
            elapsed_seconds=1_200,
        )
        is None
    )


@pytest.mark.parametrize(
    ("outcome", "elapsed_seconds"),
    (("noop_verified", 30.001), ("repaired", 60.001)),
)
def test_reuse_acceptance_rejects_public_deadline_overrun(
    outcome: str,
    elapsed_seconds: float,
) -> None:
    with pytest.raises(RelayError, match="end-to-end deadline"):
        bootstrap_reuse_acceptance_evidence(
            _receipt(outcome),
            elapsed_seconds=elapsed_seconds,
        )


@pytest.mark.parametrize(
    "operation",
    (
        "download_count",
        "payload_transfer_count",
        "payload_transfer_bytes",
        "scheduler_submission_count",
        "scheduler_cancellation_count",
        "generation_gc_count",
    ),
)
def test_reuse_acceptance_rejects_non_service_mutation(operation: str) -> None:
    receipt = _receipt()
    operations = receipt["operations"]
    assert isinstance(operations, dict)
    operations[operation] = 1

    with pytest.raises(RelayError, match=operation):
        bootstrap_reuse_acceptance_evidence(receipt, elapsed_seconds=1)


def test_exact_reuse_rejects_service_mutation() -> None:
    receipt = _receipt()
    operations = receipt["operations"]
    assert isinstance(operations, dict)
    operations["service_restart_count"] = 1

    with pytest.raises(RelayError, match="changed the managed endpoint service"):
        bootstrap_reuse_acceptance_evidence(receipt, elapsed_seconds=1)


def test_service_repair_rejects_multiple_activations() -> None:
    receipt = _receipt("repaired")
    operations = receipt["operations"]
    assert isinstance(operations, dict)
    operations["service_restart_count"] = 1

    with pytest.raises(RelayError, match="one bounded activation"):
        bootstrap_reuse_acceptance_evidence(receipt, elapsed_seconds=1)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    (
        (("jarvis_commands", "count"), 1, "invoked JARVIS"),
        (("jarvis_initialization", "action"), "initialized", "rebuilt JARVIS"),
        (("jarvis_resource_graph", "action"), "built", "rebuilt JARVIS"),
        (("jarvis_preservation", "config_byte_identical"), False, "changed JARVIS"),
        (("components", "clio-relay", "action"), "prepared", "replaced component"),
        (("service", "active_after"), False, "leave the managed endpoint persistent"),
    ),
)
def test_reuse_acceptance_rejects_hidden_rebuild_or_unready_service(
    path: tuple[str, ...],
    value: object,
    message: str,
) -> None:
    receipt = deepcopy(_receipt())
    target: dict[str, object] = receipt
    for key in path[:-1]:
        nested = target[key]
        assert isinstance(nested, dict)
        target = nested
    target[path[-1]] = value

    with pytest.raises(RelayError, match=message):
        bootstrap_reuse_acceptance_evidence(receipt, elapsed_seconds=1)

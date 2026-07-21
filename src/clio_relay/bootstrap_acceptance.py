"""Machine-readable acceptance checks for bounded bootstrap reuse."""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite
from typing import cast

from clio_relay.errors import RelayError

BOOTSTRAP_EXACT_REUSE_MAX_SECONDS = 30.0
BOOTSTRAP_SERVICE_REPAIR_MAX_SECONDS = 60.0
_REUSE_OUTCOMES = {"noop_verified", "repaired"}
_REQUIRED_REUSE_COMPONENTS = {
    "clio-relay",
    "clio-kit",
    "jarvis-cd",
    "jarvis-util",
    "frp",
    "uv",
}


def bootstrap_reuse_acceptance_evidence(
    receipt: Mapping[str, object],
    *,
    elapsed_seconds: float | int,
) -> dict[str, object] | None:
    """Validate a no-op or service-repair receipt and return bounded evidence.

    Full and relay-only installations are intentionally outside this fast-path
    gate and return ``None``. Reuse outcomes fail closed when their public
    end-to-end duration or any mutation evidence exceeds the contract.
    """
    schema_version = _required_string(receipt, "schema_version", "bootstrap receipt")
    if schema_version != "clio-relay.bootstrap-receipt.v2":
        raise RelayError(f"unsupported bootstrap receipt schema: {schema_version}")
    outcome = _required_string(receipt, "outcome", "bootstrap receipt")
    if outcome not in _REUSE_OUTCOMES:
        return None
    if type(elapsed_seconds) not in {int, float}:
        raise RelayError("bootstrap elapsed duration is invalid")
    duration = float(elapsed_seconds)
    if not isfinite(duration) or duration < 0:
        raise RelayError("bootstrap elapsed duration is invalid")
    maximum_seconds = (
        BOOTSTRAP_EXACT_REUSE_MAX_SECONDS
        if outcome == "noop_verified"
        else BOOTSTRAP_SERVICE_REPAIR_MAX_SECONDS
    )
    if duration > maximum_seconds:
        raise RelayError(
            f"{outcome} bootstrap exceeded its {maximum_seconds:g}-second "
            f"end-to-end deadline: {duration:.6f} seconds"
        )

    operations = _required_mapping(receipt, "operations", "bootstrap receipt")
    zero_operation_names = (
        "download_count",
        "payload_transfer_count",
        "payload_transfer_bytes",
        "scheduler_submission_count",
        "scheduler_cancellation_count",
        "generation_gc_count",
    )
    operation_counts = {
        name: _required_nonnegative_integer(operations, name, "bootstrap operations")
        for name in zero_operation_names
    }
    changed = [name for name, value in operation_counts.items() if value != 0]
    if changed:
        raise RelayError("bootstrap reuse performed forbidden work: " + ", ".join(changed))

    service_counts = {
        name: _required_nonnegative_integer(operations, name, "bootstrap operations")
        for name in (
            "service_start_count",
            "service_restart_count",
            "service_stop_count",
            "service_enable_count",
        )
    }
    if outcome == "noop_verified" and any(service_counts.values()):
        raise RelayError("exact bootstrap reuse changed the managed endpoint service")
    if outcome == "repaired":
        activations = (
            service_counts["service_start_count"] + service_counts["service_restart_count"]
        )
        if (
            activations != 1
            or service_counts["service_stop_count"] != 0
            or service_counts["service_enable_count"] not in {0, 1}
        ):
            raise RelayError(
                "bootstrap service repair performed work outside one bounded activation"
            )

    jarvis_commands = _required_mapping(receipt, "jarvis_commands", "bootstrap receipt")
    if _required_nonnegative_integer(jarvis_commands, "count", "JARVIS command evidence") != 0:
        raise RelayError("bootstrap reuse invoked JARVIS")
    initialization = _required_mapping(receipt, "jarvis_initialization", "bootstrap receipt")
    resource_graph = _required_mapping(receipt, "jarvis_resource_graph", "bootstrap receipt")
    if (
        _required_string(initialization, "action", "JARVIS initialization") != "preserved"
        or _required_string(resource_graph, "action", "JARVIS resource graph") != "preserved"
    ):
        raise RelayError("bootstrap reuse initialized or rebuilt JARVIS")
    preservation = _required_mapping(receipt, "jarvis_preservation", "bootstrap receipt")
    if not (
        _required_boolean(preservation, "config_byte_identical", "JARVIS preservation")
        and _required_boolean(
            preservation,
            "resource_graph_byte_identical",
            "JARVIS preservation",
        )
    ):
        raise RelayError("bootstrap reuse changed JARVIS configuration or resource graph bytes")
    repositories = _required_mapping(preservation, "repositories", "JARVIS preservation")
    repository_update = _required_mapping(
        repositories,
        "repositories",
        "JARVIS repository binding",
    )
    if (
        _required_string(repositories, "link_action", "JARVIS repository binding") != "reused"
        or _required_string(repository_update, "action", "JARVIS repository update") != "reused"
    ):
        raise RelayError("bootstrap reuse changed the JARVIS repository binding")

    components = _required_mapping(receipt, "components", "bootstrap receipt")
    if not components:
        raise RelayError("bootstrap reuse omitted component evidence")
    missing_components = sorted(_REQUIRED_REUSE_COMPONENTS - set(components))
    if missing_components:
        raise RelayError(
            "bootstrap reuse omitted required components: " + ", ".join(missing_components)
        )
    component_actions: dict[str, str] = {}
    for component_name, component_value in components.items():
        if not component_name:
            raise RelayError("bootstrap reuse component name is invalid")
        if not isinstance(component_value, Mapping):
            raise RelayError(f"bootstrap reuse component evidence is invalid: {component_name}")
        component_evidence = cast(Mapping[str, object], component_value)
        action = _required_string(
            component_evidence,
            "action",
            f"bootstrap component {component_name}",
        )
        component_actions[component_name] = action
        if action != "reused":
            raise RelayError(f"bootstrap reuse replaced component: {component_name}")

    service = _required_mapping(receipt, "service", "bootstrap receipt")
    if not (
        _required_boolean(service, "active_after", "bootstrap service")
        and _required_boolean(service, "enabled_after", "bootstrap service")
    ):
        raise RelayError("bootstrap reuse did not leave the managed endpoint persistent")

    return {
        "schema_version": "clio-relay.bootstrap-reuse-acceptance.v1",
        "outcome": outcome,
        "elapsed_seconds": duration,
        "maximum_seconds": maximum_seconds,
        "payload_free": True,
        "scheduler_untouched": True,
        "jarvis_preserved": True,
        "component_actions": component_actions,
        "service_operations": service_counts,
    }


def _required_mapping(
    value: Mapping[str, object],
    key: str,
    description: str,
) -> Mapping[str, object]:
    observed = value.get(key)
    if not isinstance(observed, Mapping):
        raise RelayError(f"{description} omitted valid {key} evidence")
    return cast(Mapping[str, object], observed)


def _required_string(value: Mapping[str, object], key: str, description: str) -> str:
    observed = value.get(key)
    if not isinstance(observed, str) or not observed:
        raise RelayError(f"{description} omitted valid {key} evidence")
    return observed


def _required_boolean(value: Mapping[str, object], key: str, description: str) -> bool:
    observed = value.get(key)
    if not isinstance(observed, bool):
        raise RelayError(f"{description} omitted valid {key} evidence")
    return observed


def _required_nonnegative_integer(
    value: Mapping[str, object],
    key: str,
    description: str,
) -> int:
    observed = value.get(key)
    if isinstance(observed, bool) or not isinstance(observed, int) or observed < 0:
        raise RelayError(f"{description} omitted valid {key} evidence")
    return observed

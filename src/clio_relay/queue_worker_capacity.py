"""Worker process-generation selection and concurrency-policy parsing.

Registered worker endpoints intentionally survive process exit, so during a
supervised restart the previous and replacement process generations can both
sit inside the freshness window at once. ``_select_active_worker_generation``
picks exactly one complete generation's slots for capacity purposes instead
of summing them together. The remaining helpers parse the
kind/workload/control-query concurrency an endpoint's registration metadata
declares, failing closed on anything malformed rather than guessing.
"""

from __future__ import annotations

from datetime import datetime
from typing import cast

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, NotFoundError
from clio_relay.models import EndpointRegistration, McpAdmissionClass
from clio_relay.worker_concurrency import kind_concurrency_metadata


def _endpoint_concurrency(metadata: dict[str, object]) -> int:
    value = metadata.get("concurrency")
    if isinstance(value, int) and value > 0:
        return value
    return 1


def _select_active_worker_generation(  # pyright: ignore[reportUnusedFunction]
    queue: ClioCoreQueue,
    endpoints: list[EndpointRegistration],
) -> tuple[
    list[EndpointRegistration],
    list[EndpointRegistration],
    str | None,
    bool | None,
    int,
]:
    """Select the newest supervised process generation and its fresh slots.

    Endpoint records intentionally survive process exit. During a systemd
    restart, the previous and replacement generations can therefore both be
    inside the freshness window. Capacity must come from exactly one complete
    parent generation rather than summing those records together.
    """
    fresh_supervisors = {
        endpoint.endpoint_id: endpoint
        for endpoint in endpoints
        if endpoint.metadata.get("worker_supervisor") is True
    }
    slots_by_parent: dict[str, list[EndpointRegistration]] = {}
    unbound_slots: list[EndpointRegistration] = []
    for endpoint in endpoints:
        if "worker_slot" not in endpoint.metadata:
            continue
        parent_endpoint_id = endpoint.metadata.get("parent_endpoint_id")
        if not isinstance(parent_endpoint_id, str) or not parent_endpoint_id:
            unbound_slots.append(endpoint)
            continue
        slots_by_parent.setdefault(parent_endpoint_id, []).append(endpoint)
    candidate_ids = set(fresh_supervisors) | set(slots_by_parent)
    if not candidate_ids:
        if unbound_slots:
            return unbound_slots, [], None, False, 0
        return [], [], None, None, 0

    candidates: list[
        tuple[datetime, str, EndpointRegistration | None, list[EndpointRegistration]]
    ] = []
    for parent_endpoint_id in candidate_ids:
        try:
            parent = fresh_supervisors.get(parent_endpoint_id) or queue.get_endpoint(
                parent_endpoint_id
            )
        except NotFoundError:
            parent = None
        slots = slots_by_parent.get(parent_endpoint_id, [])
        observed_at = (
            parent.registered_at
            if parent is not None
            else max(slot.registered_at for slot in slots)
        )
        candidates.append((observed_at, parent_endpoint_id, parent, slots))
    _observed_at, selected_id, selected_parent, selected_slots = max(
        candidates,
        key=lambda item: (item[0], item[1]),
    )
    selected_slots = sorted(
        [*selected_slots, *unbound_slots],
        key=lambda endpoint: (
            _worker_slot_index(endpoint.metadata),
            endpoint.endpoint_id,
        ),
    )
    complete = _worker_generation_is_complete(selected_parent, selected_slots)
    return (
        selected_slots,
        [] if selected_parent is None else [selected_parent],
        selected_id,
        complete,
        len(candidate_ids),
    )


def _worker_generation_is_complete(
    parent: EndpointRegistration | None,
    slots: list[EndpointRegistration],
) -> bool:
    """Require every declared slot from one exact supervisor generation."""
    if parent is None or parent.metadata.get("worker_supervisor") is not True:
        return False
    expected_concurrency = _endpoint_concurrency(parent.metadata)
    if expected_concurrency < 2 or len(slots) != expected_concurrency:
        return False
    indices = [_worker_slot_index(endpoint.metadata) for endpoint in slots]
    if indices != list(range(expected_concurrency)):
        return False
    return all(
        endpoint.metadata.get("parent_endpoint_id") == parent.endpoint_id
        and _endpoint_concurrency(endpoint.metadata) == 1
        and endpoint.hostname == parent.hostname
        and endpoint.pid == parent.pid
        and endpoint.registered_at >= parent.registered_at
        for endpoint in slots
    )


def _worker_slot_index(metadata: dict[str, object]) -> int:
    """Return a sortable slot index while keeping malformed metadata invalid."""
    value = metadata.get("worker_slot")
    return value if type(value) is int and value >= 0 else 2**63 - 1


def _endpoint_lane_configuration(  # pyright: ignore[reportUnusedFunction]
    metadata: dict[str, object],
) -> tuple[int, int] | None:
    """Return one explicit workload/control slot declaration, or fail closed."""
    workload = metadata.get("workload_concurrency")
    control = metadata.get("control_query_concurrency")
    if (
        type(workload) is not int
        or type(control) is not int
        or workload < 0
        or control < 0
        or workload + control != _endpoint_concurrency(metadata)
    ):
        return None
    admission_class = metadata.get("mcp_admission_class")
    if admission_class is not None:
        if admission_class not in {
            McpAdmissionClass.WORKLOAD.value,
            McpAdmissionClass.CONTROL_QUERY.value,
        }:
            return None
        expected = (1, 0) if admission_class == McpAdmissionClass.WORKLOAD.value else (0, 1)
        if (workload, control) != expected:
            return None
    return workload, control


def _kind_concurrency_configurations(  # pyright: ignore[reportUnusedFunction]
    endpoints: list[EndpointRegistration],
) -> tuple[list[dict[str, int]], bool]:
    configurations: list[dict[str, int]] = []
    seen: set[tuple[tuple[str, int], ...]] = set()
    valid = True
    for endpoint in endpoints:
        raw = endpoint.metadata.get("kind_concurrency", {})
        if not isinstance(raw, dict):
            valid = False
            continue
        try:
            configuration = kind_concurrency_metadata(cast(dict[str, int], raw))
        except ConfigurationError:
            valid = False
            continue
        key = tuple(configuration.items())
        if key in seen:
            continue
        seen.add(key)
        configurations.append(configuration)
    return configurations, valid

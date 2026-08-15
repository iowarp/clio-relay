"""In-memory validation and durable state for queue index migration."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import cast

from clio_relay import (
    queue_layout,
    queue_lease_records,
    queue_store_lock,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause

logger = logging.getLogger(__name__)

_CheckpointValidator = Callable[..., dict[str, object]]
_DocumentReader = Callable[[Path], object]


def require_sealed_checkpoint_group(
    raw: object,
    *,
    label: str,
    families: tuple[str, ...],
    checkpoint_validator: _CheckpointValidator,
    schema_by_family: dict[str, str] | None = None,
) -> dict[str, object]:
    """Validate one exact fixed-family checkpoint group."""
    if not isinstance(raw, dict):
        raise QueueConflictError(f"sealed {label} checkpoints are not an object")
    group = cast(dict[str, object], raw)
    if set(group) != set(families):
        raise QueueConflictError(f"sealed {label} checkpoints have an unknown shape")
    schemas = schema_by_family or {}
    for family in families:
        checkpoint_validator(
            group[family],
            label=f"{label} {family}",
            schema_version=schemas.get(family),
        )
    return group


def require_optional_bounded_record_count(
    checkpoint: dict[str, object],
    *,
    label: str,
) -> None:
    """Validate an optional bounded migration record count."""
    record_count = checkpoint.get("record_count")
    if record_count is None:
        return
    if (
        isinstance(record_count, bool)
        or not isinstance(record_count, int)
        or not 0 <= record_count <= queue_layout.MAX_LIVE_LEASE_RECORDS
    ):
        raise QueueConflictError(f"sealed {label} record count is invalid")


def read_sealed_index_migration_state(
    storage_root: Path,
    *,
    checkpoint_validator: _CheckpointValidator,
    document_reader: _DocumentReader,
    allow_legacy_lease_schema: bool = False,
) -> dict[str, object]:
    """Read and strictly validate indexed-era state without repairing or scanning."""
    path = storage_root / "migrations" / "index-v1.json"
    try:
        raw_state = document_reader(path)
        if not isinstance(raw_state, dict):
            raise QueueConflictError("sealed index migration state is not an object")
        state = cast(dict[str, object], raw_state)
        if state.get("schema_version") != queue_layout.INDEX_MIGRATION_SCHEMA:
            raise QueueConflictError("sealed index migration state schema is unsupported")
        expected_keys = {
            "schema_version",
            "complete",
            "families",
            "finalize",
            "order_families",
            "global_order_families",
            "retention_families",
            "operational_families",
            "lease_operational_repair",
            "lease_capacity_aggregate",
        }
        if set(state) != expected_keys or not isinstance(state.get("complete"), bool):
            raise QueueConflictError("sealed index migration state has an unknown shape")
        require_sealed_checkpoint_group(
            state.get("families"),
            label="canonical family",
            families=("jobs", "tasks", "leases", "artifacts", "progress"),
            checkpoint_validator=checkpoint_validator,
        )
        checkpoint_validator(state.get("finalize"), label="finalize")
        require_sealed_checkpoint_group(
            state.get("order_families"),
            label="order family",
            families=queue_store_lock.ORDER_FAMILIES,
            checkpoint_validator=checkpoint_validator,
        )
        require_sealed_checkpoint_group(
            state.get("global_order_families"),
            label="global-order family",
            families=queue_store_lock.GLOBAL_ORDER_FAMILIES,
            checkpoint_validator=checkpoint_validator,
        )
        require_sealed_checkpoint_group(
            state.get("retention_families"),
            label="retention family",
            families=queue_store_lock.RETENTION_INDEX_FAMILIES,
            checkpoint_validator=checkpoint_validator,
        )
        raw_operational: object = state.get("operational_families")
        operational = (
            cast(dict[str, object], raw_operational) if isinstance(raw_operational, dict) else {}
        )
        raw_lease_checkpoint = operational.get("leases")
        lease_checkpoint = (
            cast(dict[str, object], raw_lease_checkpoint)
            if isinstance(raw_lease_checkpoint, dict)
            else {}
        )
        lease_schema = lease_checkpoint.get("schema_version")
        accepted_lease_schema = queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA
        if (
            allow_legacy_lease_schema
            and lease_schema == queue_layout.LEGACY_LEASE_OPERATIONAL_INDEX_SCHEMA
        ):
            accepted_lease_schema = queue_layout.LEGACY_LEASE_OPERATIONAL_INDEX_SCHEMA
        require_sealed_checkpoint_group(
            cast(object, raw_operational),
            label="operational family",
            families=queue_store_lock.OPERATIONAL_INDEX_FAMILIES,
            schema_by_family={"leases": accepted_lease_schema},
            checkpoint_validator=checkpoint_validator,
        )
        raw_repair = state.get("lease_operational_repair")
        if not isinstance(raw_repair, dict):
            raise QueueConflictError("sealed lease repair checkpoint is not an object")
        repair = cast(dict[str, object], raw_repair)
        if set(repair) not in (
            {"complete", "schema_version"},
            {"complete", "schema_version", "record_count"},
        ):
            raise QueueConflictError("sealed lease repair checkpoint has an unknown shape")
        if (
            not isinstance(repair.get("complete"), bool)
            or repair.get("schema_version") != accepted_lease_schema
        ):
            raise QueueConflictError("sealed lease repair checkpoint is invalid")
        require_optional_bounded_record_count(repair, label="lease repair")

        raw_capacity = state.get("lease_capacity_aggregate")
        if not isinstance(raw_capacity, dict):
            raise QueueConflictError("sealed lease capacity checkpoint is not an object")
        capacity = cast(dict[str, object], raw_capacity)
        if not isinstance(capacity.get("complete"), bool):
            raise QueueConflictError("sealed lease capacity completion is invalid")
        capacity_complete = capacity["complete"] is True
        capacity_keys = {"complete", "schema_version"}
        if capacity_complete:
            capacity_keys.update({"epoch_id", "generation", "record_count"})
        if set(capacity) != capacity_keys:
            raise QueueConflictError("sealed lease capacity checkpoint has an unknown shape")
        generation = capacity.get("generation")
        if capacity.get("schema_version") != queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA or (
            capacity_complete
            and (
                not queue_lease_records.is_capacity_identity(capacity.get("epoch_id"))
                or isinstance(generation, bool)
                or not isinstance(generation, int)
                or generation < 0
            )
        ):
            raise QueueConflictError("sealed lease capacity checkpoint is invalid")
        require_optional_bounded_record_count(capacity, label="lease capacity")
        if state.get("complete") is True and not index_migration_components_complete(state):
            raise QueueConflictError("sealed complete index migration has incomplete components")
    except (OSError, ValueError, QueueConflictError) as error:
        raise queue_store_lock.LegacyQueueStateError(
            family="migrations",
            path=path,
            reason=f"sealed index migration state is invalid: {type(error).__name__}",
        ) from error
    return state


def read_index_migration_state(storage_root: Path) -> dict[str, object]:
    """Read the durable index migration document."""
    path = storage_root / "migrations" / "index-v1.json"
    try:
        raw = queue_store_read.read_json_document(path)
    except (OSError, QueueConflictError) as exc:
        raise queue_conflict_from_cause(
            f"invalid index migration state {path}",
            cause=exc,
            logger=logger,
        ) from exc
    if not isinstance(raw, dict):
        raise QueueConflictError(f"index migration state is not an object: {path}")
    state = cast(dict[str, object], raw)
    if state.get("schema_version") != queue_layout.INDEX_MIGRATION_SCHEMA:
        raise QueueConflictError(f"unsupported index migration state: {path}")
    return state


def write_index_migration_state(storage_root: Path, state: dict[str, object]) -> None:
    """Persist the durable index migration document."""
    queue_store_write.write_json(storage_root, storage_root / "migrations" / "index-v1.json", state)


def require_index_migration_complete(storage_root: Path) -> None:
    """Require every durable index migration component to be complete."""
    if read_index_migration_state(storage_root).get("complete") is not True:
        raise QueueConflictError(
            "queue indexes require migration; run `clio-relay queue migrate-indexes` "
            "before starting workers"
        )


def index_integer(index: dict[str, object], field: str) -> int:
    """Return one validated non-negative integer from an in-memory job index."""
    value = index.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise QueueConflictError(f"invalid job index integer: {field}")
    return value


def index_migration_components_complete(state: dict[str, object]) -> bool:
    """Return whether every independently replayable index checkpoint is complete."""
    for field in (
        "families",
        "order_families",
        "global_order_families",
        "retention_families",
        "operational_families",
    ):
        raw_family = state.get(field)
        if not isinstance(raw_family, dict):
            return False
        if any(
            not isinstance(raw_checkpoint, dict)
            or cast(dict[str, object], raw_checkpoint).get("complete") is not True
            for raw_checkpoint in cast(dict[str, object], raw_family).values()
        ):
            return False
    for field in ("finalize", "lease_operational_repair", "lease_capacity_aggregate"):
        raw_checkpoint = state.get(field)
        if (
            not isinstance(raw_checkpoint, dict)
            or cast(dict[str, object], raw_checkpoint).get("complete") is not True
        ):
            return False
    return True

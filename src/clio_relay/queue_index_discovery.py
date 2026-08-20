"""Index-migration state discovery: schema upgrade and gate reconciliation.

Owns the three bounded, no-history-scan repairs that widen or downgrade the
durable v0.9->indexed migration checkpoint document without ever touching
canonical record history: ``_upgrade_sealed_lease_operational_schema_unlocked``
(invalidate a stale v1 lease-index schema so the v2 repair can rebuild it),
``_reconcile_sealed_lease_capacity_gate_unlocked`` (downgrade a sealed lease-
capacity gate when its aggregate/checkpoint pair no longer agrees with the
canonical fixed-capacity record), and ``_ensure_extended_migration_state``
(discover checkpoint families a later schema version added and extend the
migration document with them in place, idempotently).

Predecessors: CQ3 (store lock/read/write), CQ5 (index state), CQ15 (lease
capacity state/audit -- ``_read_lease_capacity_aggregate_unlocked``), all
landed. Every collaborator below is reached through an ordinary inherited
``self.`` call (a forward edge to an earlier-ranked owner, stubbed under
``TYPE_CHECKING``) or a module-qualified lookup already established by an
earlier slice (``queue_lease_indexes.lease_operational_records_present``,
``queue_lease_records.is_capacity_identity``).

CQ19-TI-01 typed deviation (documented on ``queue_startup.py`` and in the
design doc ledger §14): the migration-state read/write pair
(``_read_index_migration_state``/``_write_index_migration_state``), the
completeness gate (``_require_index_migration_complete``), the lease-capacity
completeness check (``_lease_capacity_migration_complete_unlocked``), and the
write-ahead intent primitives (``_write_transition_intent_unlocked``,
``_recover_pending_transitions_unlocked``) all stay facade-resident rather
than moving into any CQ19 owner: each is self-called by many already-landed
owners spanning ranks 18-40 (``queue_execution_cleanup`` at rank 23 is the
earliest), so extracting any of them into a rank-42+ owner would create a
reverse-rank edge the architecture guard rejects, and no earlier-ranked owner
has the headroom to host all of them. This module's three methods below have
no such inbound edge from any other owner (verified: zero external callers),
so they extract cleanly at rank 42.

CQ20 dissolution: the facade's own ``_read_sealed_index_migration_state``
(a thin ``queue_index_state.read_sealed_index_migration_state`` forward,
left behind when CQ19-TI-01 was written) is deleted outright rather than
moved -- ``queue_legacy_audit.QueueLegacyAuditMixin`` (rank 12) already owns
a byte-identical private equivalent, ``_read_sealed_state`` (same forward,
its own per-owner ``_unique_json`` document reader). Both of this module's
two real external callers (this file's own
``_upgrade_sealed_lease_operational_schema_unlocked``, and
``queue_startup.initialize``) now call ``self._read_sealed_state(...)``
directly -- a forward self-call to the earlier-ranked owner, exactly the
CQ9-ledger §9.3 "hoist to whichever side the topology requires" precedent,
except here the target already existed and needed no new duplicate at all.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

from clio_relay import (
    queue_context,
    queue_layout,
    queue_lease_indexes,
    queue_lease_records,
    queue_store_lock,
    queue_store_read,
)
from clio_relay.errors import QueueConflictError

if TYPE_CHECKING:
    from clio_relay.queue_lease_records import LeaseCapacityPair as _LeaseCapacityPair


class QueueIndexDiscoveryMixin:
    """Own bounded index-migration-state discovery and schema/gate repair."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol

    if TYPE_CHECKING:

        def _read_index_migration_state(self) -> dict[str, object]: ...
        def _write_index_migration_state(self, state: dict[str, object]) -> None: ...
        def _read_sealed_state(
            self,
            *,
            allow_legacy_lease_schema: bool = False,
        ) -> dict[str, object]: ...
        def _read_lease_capacity_aggregate_unlocked(self) -> _LeaseCapacityPair: ...

    def _upgrade_sealed_lease_operational_schema_unlocked(self) -> None:
        """Invalidate exact v1 lease indexes so the bounded v2 migration can rebuild them."""
        if not self._lock.is_locked:
            raise RuntimeError("sealed lease index upgrade requires the queue lock")
        state = self._read_sealed_state(allow_legacy_lease_schema=True)
        operational = cast(dict[str, object], state["operational_families"])
        lease_checkpoint = cast(dict[str, object], operational["leases"])
        repair = cast(dict[str, object], state["lease_operational_repair"])
        if lease_checkpoint.get("schema_version") == queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA:
            return
        lease_checkpoint.clear()
        lease_checkpoint.update(
            {
                "cursor": None,
                "complete": False,
                "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
            }
        )
        repair.clear()
        repair.update(
            {
                "complete": False,
                "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
            }
        )
        state["complete"] = False
        self._write_index_migration_state(state)

    def _reconcile_sealed_lease_capacity_gate_unlocked(self) -> None:
        """Downgrade a sealed migration gate when its fixed capacity pair is corrupt."""
        state = self._read_index_migration_state()
        raw_checkpoint = state.get("lease_capacity_aggregate")
        if not isinstance(raw_checkpoint, dict):
            return
        checkpoint = cast(dict[str, object], raw_checkpoint)
        if checkpoint.get("complete") is not True:
            return
        try:
            current = self._read_lease_capacity_aggregate_unlocked()
        except (OSError, QueueConflictError):
            current = None
        migrated_generation = checkpoint.get("generation")
        valid = (
            current is not None
            and current.aggregate.epoch_id == checkpoint.get("epoch_id")
            and isinstance(migrated_generation, int)
            and not isinstance(migrated_generation, bool)
            and current.aggregate.generation >= migrated_generation
        )
        if valid:
            return
        checkpoint.clear()
        checkpoint.update(
            {
                "complete": False,
                "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
            }
        )
        state["complete"] = False
        self._write_index_migration_state(state)

    def _ensure_extended_migration_state(self) -> None:
        """Discover checkpoint families a later schema added and extend the state."""
        state = self._read_index_migration_state()
        changed = False
        if not isinstance(state.get("order_families"), dict):
            state["order_families"] = {
                family: {
                    "cursor": None,
                    "complete": next((self._storage_root / family).glob("*.json"), None) is None,
                }
                for family in queue_store_lock.ORDER_FAMILIES
            }
            changed = True
        if not isinstance(state.get("retention_families"), dict):
            state["retention_families"] = {
                family: {
                    "cursor": None,
                    "complete": next((self._storage_root / family).glob("*.json"), None) is None,
                }
                for family in queue_store_lock.RETENTION_INDEX_FAMILIES
            }
            changed = True
        if not isinstance(state.get("global_order_families"), dict):
            state["global_order_families"] = {
                family: {
                    "cursor": None,
                    "complete": next((self._storage_root / family).glob("*.json"), None) is None,
                }
                for family in queue_store_lock.GLOBAL_ORDER_FAMILIES
            }
            changed = True
        else:
            global_order_state = cast(
                dict[str, object],
                state["global_order_families"],
            )
            for family in queue_store_lock.GLOBAL_ORDER_FAMILIES:
                if not isinstance(global_order_state.get(family), dict):
                    global_order_state[family] = {
                        "cursor": None,
                        "complete": next(
                            (self._storage_root / family).glob("*.json"),
                            None,
                        )
                        is None,
                    }
                    changed = True
        if not isinstance(state.get("operational_families"), dict):
            state["operational_families"] = {
                family: {
                    "cursor": None,
                    "complete": next((self._storage_root / family).glob("*.json"), None) is None,
                    **(
                        {"schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA}
                        if family == "leases"
                        else {}
                    ),
                }
                for family in queue_store_lock.OPERATIONAL_INDEX_FAMILIES
            }
            changed = True
        else:
            operational_state = cast(dict[str, object], state["operational_families"])
            for family in queue_store_lock.OPERATIONAL_INDEX_FAMILIES:
                if not isinstance(operational_state.get(family), dict):
                    operational_state[family] = {
                        "cursor": None,
                        "complete": next((self._storage_root / family).glob("*.json"), None)
                        is None,
                        **(
                            {"schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA}
                            if family == "leases"
                            else {}
                        ),
                    }
                    changed = True
            raw_lease_checkpoint = operational_state.get("leases")
            if isinstance(raw_lease_checkpoint, dict):
                lease_checkpoint = cast(dict[str, object], raw_lease_checkpoint)
                if (
                    lease_checkpoint.get("schema_version")
                    != queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA
                ):
                    lease_checkpoint.update(
                        {
                            "cursor": None,
                            "complete": next(
                                (self._storage_root / "leases").glob("*.json"),
                                None,
                            )
                            is None,
                            "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
                        }
                    )
                    changed = True
        if not isinstance(state.get("lease_operational_repair"), dict):
            state["lease_operational_repair"] = {
                "complete": not queue_lease_indexes.lease_operational_records_present(
                    self._storage_root
                ),
                "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
            }
            changed = True
        else:
            raw_lease_repair = cast(
                dict[str, object],
                state["lease_operational_repair"],
            )
            if (
                raw_lease_repair.get("schema_version")
                != queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA
            ):
                raw_lease_repair.update(
                    {
                        "complete": False,
                        "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
                    }
                )
                changed = True
        pending_transition = bool(
            queue_store_read.bounded_json_record_paths(
                self._storage_root / "transition_intents",
                limit=queue_layout.MAX_TRANSITION_INTENT_RECORDS,
                label="queue transition intent directory",
            )
        )
        raw_capacity = state.get("lease_capacity_aggregate")
        if not isinstance(raw_capacity, dict):
            state["lease_capacity_aggregate"] = {
                "complete": False,
                "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
            }
            changed = True
        else:
            capacity_checkpoint = cast(dict[str, object], raw_capacity)
            complete = capacity_checkpoint.get("complete") is True
            valid_complete_fields = (
                queue_lease_records.is_capacity_identity(capacity_checkpoint.get("epoch_id"))
                and isinstance(capacity_checkpoint.get("generation"), int)
                and not isinstance(capacity_checkpoint.get("generation"), bool)
                and cast(int, capacity_checkpoint.get("generation")) >= 0
                and isinstance(capacity_checkpoint.get("record_count"), int)
                and not isinstance(capacity_checkpoint.get("record_count"), bool)
                and 0
                <= cast(int, capacity_checkpoint.get("record_count"))
                <= queue_layout.MAX_LIVE_LEASE_RECORDS
            )
            if capacity_checkpoint.get(
                "schema_version"
            ) != queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA or (
                complete and not valid_complete_fields
            ):
                state["lease_capacity_aggregate"] = {
                    "complete": False,
                    "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
                }
                changed = True
            elif complete:
                try:
                    current_capacity = self._read_lease_capacity_aggregate_unlocked()
                except (OSError, QueueConflictError):
                    if not pending_transition:
                        capacity_checkpoint.clear()
                        capacity_checkpoint.update(
                            {
                                "complete": False,
                                "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
                            }
                        )
                        changed = True
                else:
                    migrated_generation = cast(int, capacity_checkpoint["generation"])
                    if (
                        current_capacity.aggregate.epoch_id != capacity_checkpoint.get("epoch_id")
                        or current_capacity.aggregate.generation < migrated_generation
                    ) and not pending_transition:
                        capacity_checkpoint.clear()
                        capacity_checkpoint.update(
                            {
                                "complete": False,
                                "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
                            }
                        )
                        changed = True
        raw_order = cast(dict[str, object], state["order_families"])
        raw_retention = cast(dict[str, object], state["retention_families"])
        raw_global_order = cast(dict[str, object], state["global_order_families"])
        raw_operational = cast(dict[str, object], state["operational_families"])
        raw_lease_repair = cast(dict[str, object], state["lease_operational_repair"])
        raw_capacity = cast(dict[str, object], state["lease_capacity_aggregate"])
        incomplete = False
        for raw_checkpoint in (
            *raw_order.values(),
            *raw_global_order.values(),
            *raw_retention.values(),
            *raw_operational.values(),
        ):
            if not isinstance(raw_checkpoint, dict):
                incomplete = True
                break
            checkpoint = cast(dict[str, object], raw_checkpoint)
            if checkpoint.get("complete") is not True:
                incomplete = True
                break
        if raw_lease_repair.get("complete") is not True:
            incomplete = True
        if raw_capacity.get("complete") is not True:
            incomplete = True
        if incomplete and state.get("complete") is True:
            state["complete"] = False
            changed = True
        if changed:
            self._write_index_migration_state(state)

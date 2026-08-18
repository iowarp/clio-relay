"""Durable monitor-rule ownership: creation, listing, and updates.

Owns every public method that creates, lists, pages, scans, or updates a
``MonitorRule``, plus the per-job/active-rule index it keeps converged.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel

from clio_relay import (
    queue_context,
    queue_index_state,
    queue_layout,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import MonitorRule, RelayEvent, RelayJob


class QueueMonitorRulesMixin:
    """Own durable monitor-rule records and their job-scoped indexes."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        def _job_index_exists(self, job_id: str) -> bool: ...
        def _ensure_global_order_entry_unlocked(self, family: str, record_id: str) -> int: ...
        def get_job(self, job_id: str) -> RelayJob: ...
        def append_event(
            self,
            job_id: str,
            event_type: str,
            message: str,
            *,
            locked: bool = False,
            payload: dict[str, object] | None = None,
        ) -> RelayEvent: ...
        def _read_global_order_page[RecordT: BaseModel](
            self,
            *,
            family: str,
            model: type[RecordT],
            identity_field: str,
            cursor: int,
            limit: int,
            predicate: Callable[[RecordT], bool] | None = None,
        ) -> tuple[list[RecordT], int | None, int]: ...
        def _scan_global_order[RecordT: BaseModel](
            self,
            *,
            family: str,
            model: type[RecordT],
            identity_field: str,
            limit: int,
            predicate: Callable[[RecordT], bool] | None = None,
        ) -> tuple[list[RecordT], bool]: ...

    def append_monitor_rule(self, rule: MonitorRule) -> MonitorRule:
        """Create a durable monitor rule."""
        queue_layout.QueueLayout.require_durable_record_id(rule.rule_id, field="rule_id")
        queue_layout.QueueLayout.require_durable_record_id(rule.job_id, field="job_id")
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            self.get_job(rule.job_id)
            self._ensure_global_order_entry_unlocked("monitor_rules", rule.rule_id)
            queue_store_write.write_model(
                self._storage_root,
                self._storage_root / "monitor_rules" / f"{rule.rule_id}.json",
                rule,
            )
            self._sync_monitor_rule_indexes_unlocked(rule)
            self.append_event(
                rule.job_id,
                "monitor.rule.created",
                f"Monitor rule created: {rule.rule_id}",
                locked=True,
                payload={"rule_id": rule.rule_id, "pattern": rule.pattern},
            )
        return rule

    def list_monitor_rules(self, job_id: str | None = None) -> list[MonitorRule]:
        """Return monitor rules, optionally filtered by job id."""
        if job_id is not None:
            job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        if job_id is not None and self._job_index_exists(job_id):
            job_key = queue_layout.QueueLayout.durable_key(job_id)
            rules = list(
                queue_store_read.read_many(
                    self._storage_root / "monitor_rules_by_job" / job_key,
                    MonitorRule,
                    identity_field="rule_id",
                )
            )
        else:
            rules = list(
                queue_store_read.read_many(
                    self._storage_root / "monitor_rules",
                    MonitorRule,
                    identity_field="rule_id",
                )
            )
            if job_id is not None:
                rules = [rule for rule in rules if rule.job_id == job_id]
        return sorted(rules, key=lambda rule: rule.created_at)

    def list_monitor_rules_page(
        self,
        *,
        cursor: int = 1,
        limit: int = 100,
        job_id: str | None = None,
        enabled: bool | None = None,
    ) -> tuple[list[MonitorRule], int | None, int]:
        """Read one global monitor-rule source window with in-window filters."""
        if job_id is not None:
            job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")

        def matches(rule: MonitorRule) -> bool:
            return (job_id is None or rule.job_id == job_id) and (
                enabled is None or rule.enabled is enabled
            )

        return self._read_global_order_page(
            family="monitor_rules",
            model=MonitorRule,
            identity_field="rule_id",
            cursor=cursor,
            limit=limit,
            predicate=matches,
        )

    def scan_monitor_rules(
        self,
        *,
        limit: int,
        job_id: str | None = None,
        enabled: bool | None = None,
    ) -> tuple[list[MonitorRule], bool]:
        """Read one bounded monitor-rule source window and truncation state."""
        if job_id is not None:
            job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")

        def matches(rule: MonitorRule) -> bool:
            return (job_id is None or rule.job_id == job_id) and (
                enabled is None or rule.enabled is enabled
            )

        return self._scan_global_order(
            family="monitor_rules",
            model=MonitorRule,
            identity_field="rule_id",
            limit=limit,
            predicate=matches,
        )

    def update_monitor_rule(self, rule: MonitorRule) -> MonitorRule:
        """Persist a monitor rule update."""
        queue_layout.QueueLayout.require_durable_record_id(rule.rule_id, field="rule_id")
        queue_layout.QueueLayout.require_durable_record_id(rule.job_id, field="job_id")
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            existing = queue_store_read.read_optional(
                self._storage_root,
                self._storage_root / "monitor_rules" / f"{rule.rule_id}.json",
                MonitorRule,
            )
            if existing is None:
                raise NotFoundError(f"monitor rule not found: {rule.rule_id}")
            if existing.rule_id != rule.rule_id:
                raise QueueConflictError(
                    f"canonical monitor rule identity mismatch: {rule.rule_id}"
                )
            if existing.job_id != rule.job_id:
                raise QueueConflictError(f"monitor rule cannot change job: {rule.rule_id}")
            self._ensure_global_order_entry_unlocked("monitor_rules", rule.rule_id)
            queue_store_write.write_model(
                self._storage_root,
                self._storage_root / "monitor_rules" / f"{rule.rule_id}.json",
                rule,
            )
            self._sync_monitor_rule_indexes_unlocked(rule)
        return rule

    def _sync_monitor_rule_indexes_unlocked(self, rule: MonitorRule) -> None:
        indexed_path = self._job_record_path(
            "monitor_rules_by_job",
            rule.job_id,
            rule.rule_id,
        )
        active_path = self._job_record_path(
            "active_monitor_rules_by_job",
            rule.job_id,
            rule.rule_id,
        )
        queue_store_write.write_model(self._storage_root, indexed_path, rule)
        if rule.enabled and rule.triggered_at is None:
            queue_store_write.write_model(self._storage_root, active_path, rule)
        else:
            queue_store_write.unlink_durable_path(active_path, missing_ok=True)

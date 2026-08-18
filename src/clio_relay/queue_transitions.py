"""The transition-intent applier: replay every interrupted queue transition.

Owns ``_reconcile_transition_intents_unlocked``, the bounded write-ahead-log
replay that recovers a crash between a canonical write and its dependent
index/capacity convergence. It is the last piece of the queue-owner split:
every ``kind`` branch below dispatches into an already-landed owner's real
mutation primitive (lease sync/delete, stale-lease recovery, lease-index
repair, lease acquisition, job/task/gateway derived-index sync), so this
owner must rank after all of them.

Predecessors: CQ2-CQ18 (all landed; the ``lease_sync``/``lease_delete``
branches alone reach CQ4/CQ9/CQ12/CQ15/CQ16-landed collaborators). Every
dispatch below is an ordinary inherited ``self.`` call to an earlier-ranked
owner (a forward edge, stubbed under ``TYPE_CHECKING``).

CQ19-TI-01 (see ``queue_index_discovery.py``'s module docstring): the public
entrypoints that call into this owner -- ``reconcile_pending_transitions``,
``_recover_pending_transitions_unlocked`` (a thin one-line wrapper around
this module's own method), and the write-side counterpart ``_write_
transition_intent_unlocked`` -- all stay facade-resident, since each is
self-called by many already-landed owners spanning ranks 18-40 and none of
those calls are owned by any ``queue_*.py`` mixin (so they carry no
architecture-guard edge regardless of this owner's rank).

Failing-first sabotage (design doc CQ19 row, "one transition-applier
lookup"): the journal listing is resolved through ``queue_transitions.
queue_store_read.bounded_json_record_paths`` -- an isolated-namespace patch
on that lookup fails the replay before it reads a single intent. The
per-intent document read (``queue_store_read.read_json_document``) and the
processed-intent unlink (``queue_store_write.unlink_durable_path``) are the
same module-qualified idiom, matching how ``core_queue.py`` already called
the latter through its own private wrapper before this move.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

from clio_relay import queue_layout, queue_lease_records, queue_store_read, queue_store_write
from clio_relay.errors import QueueConflictError
from clio_relay.models import Lease, RelayJob, RelayTask

if TYPE_CHECKING:
    from pathlib import Path

    from pydantic import BaseModel

    from clio_relay import queue_context


class QueueTransitionsMixin:
    """Own the bounded write-ahead-log replay for interrupted queue transitions."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol

    if TYPE_CHECKING:

        def _write(self, path: Path, record: BaseModel) -> None: ...
        def _read_optional[Record: BaseModel](
            self, path: Path, model: type[Record]
        ) -> Record | None: ...
        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        def _apply_lease_index_repair_intent_unlocked(
            self,
            intent_path: Path,
            payload: dict[str, object],
        ) -> int: ...
        def _reconcile_lease_acquire_intent_unlocked(
            self,
            path: Path,
            payload: dict[str, object],
        ) -> None: ...
        def _sync_lease_operational_indexes_unlocked(
            self, lease: Lease, *, job: RelayJob, previous_lease: Lease | None = None
        ) -> queue_lease_records.LeaseIndexIdentity: ...
        def _apply_lease_capacity_transition_unlocked(
            self, transition_value: object, *, target: Literal["after", "rollback"], label: str
        ) -> queue_lease_records.LeaseCapacityPair: ...
        def _before_lease_capacity_intent_removal(self, _kind: str, _path: Path) -> None: ...
        def _lease_capacity_migration_complete_unlocked(self) -> bool: ...
        def _validate_lease_index_identity(
            self,
            lease: Lease,
            identity: queue_lease_records.LeaseIndexIdentity,
        ) -> None: ...
        def _delete_lease_operational_indexes_unlocked(
            self,
            identity: queue_lease_records.LeaseIndexIdentity,
            *,
            allow_foreign_manifest: bool = False,
        ) -> None: ...
        def _apply_stale_lease_recovery_intent_unlocked(
            self,
            intent_path: Path,
            payload: dict[str, object],
        ) -> RelayJob: ...
        def _sync_job_derived_unlocked(self, job: RelayJob) -> None: ...
        def _sync_task_derived_unlocked(self, task: RelayTask) -> None: ...
        def _sync_gateway_session_derived_unlocked(self, session_id: str) -> None: ...

    def _reconcile_transition_intents_unlocked(self) -> list[RelayJob]:
        """Replay interrupted queue transitions from canonical records or exact intents."""
        paths = queue_store_read.bounded_json_record_paths(
            self._storage_root / "transition_intents",
            limit=queue_layout.MAX_TRANSITION_INTENT_RECORDS,
            label="queue transition intent directory",
        )
        intents: list[tuple[Path, dict[str, object]]] = []
        recovered_stale_jobs: list[RelayJob] = []
        for path in paths:
            raw = queue_store_read.read_json_document(path)
            if not isinstance(raw, dict):
                raise QueueConflictError(f"queue transition intent is not an object: {path}")
            intent = cast(dict[str, object], raw)
            if intent.get("schema_version") != "clio-relay.queue-transition-intent.v1":
                raise QueueConflictError(f"unsupported queue transition intent: {path}")
            if not isinstance(intent.get("kind"), str) or not isinstance(
                intent.get("payload"), dict
            ):
                raise QueueConflictError(f"invalid queue transition intent: {path}")
            intents.append((path, intent))

        order = {
            "lease_index_repair": 0,
            "lease_acquire": 1,
            "lease_sync": 2,
            "lease_delete": 3,
            "stale_lease_recovery": 4,
            "job_sync": 5,
            "task_sync": 6,
            "gateway_sync": 7,
        }
        for path, intent in sorted(
            intents,
            key=lambda item: order.get(cast(str, item[1]["kind"]), 99),
        ):
            kind = cast(str, intent["kind"])
            payload = cast(dict[str, object], intent["payload"])
            if kind == "lease_index_repair":
                self._apply_lease_index_repair_intent_unlocked(path, payload)
                continue
            if kind == "lease_acquire":
                self._reconcile_lease_acquire_intent_unlocked(path, payload)
                continue
            if kind == "lease_sync":
                lease = Lease.model_validate(payload.get("lease"))
                previous = Lease.model_validate(payload.get("previous_lease"))
                job = RelayJob.model_validate(payload.get("job"))
                if lease.job_id != job.job_id or previous.lease_id != lease.lease_id:
                    raise QueueConflictError(f"lease synchronization identity mismatch: {path}")
                self._write(self._storage_root / "leases" / f"{lease.lease_id}.json", lease)
                self._write(
                    self._job_record_path("leases_by_job", lease.job_id, lease.lease_id),
                    lease,
                )
                self._sync_lease_operational_indexes_unlocked(
                    lease,
                    job=job,
                    previous_lease=previous,
                )
                capacity_transition = payload.get("lease_capacity_transition")
                if capacity_transition is not None:
                    self._apply_lease_capacity_transition_unlocked(
                        capacity_transition,
                        target="after",
                        label=f"lease synchronization {lease.lease_id}",
                    )
                    self._before_lease_capacity_intent_removal("lease_sync", path)
                elif self._lease_capacity_migration_complete_unlocked():
                    raise QueueConflictError(
                        f"lease synchronization intent has no capacity transition: {path}"
                    )
                queue_store_write.unlink_durable_path(path, missing_ok=True)
                continue
            if kind == "lease_delete":
                lease_id = payload.get("lease_id")
                job_id = payload.get("job_id")
                if (
                    not isinstance(lease_id, str)
                    or not lease_id
                    or not isinstance(job_id, str)
                    or not job_id
                ):
                    raise QueueConflictError(f"invalid lease deletion intent: {path}")
                lease: Lease | None = None
                identity: queue_lease_records.LeaseIndexIdentity | None = None
                if payload.get("lease") is not None or payload.get("index") is not None:
                    lease = Lease.model_validate(payload.get("lease"))
                    identity = queue_lease_records.lease_index_identity_from_document(
                        payload.get("index"),
                        label=f"lease deletion index {path}",
                    )
                    self._validate_lease_index_identity(lease, identity)
                    if lease_id != lease.lease_id or job_id != lease.job_id:
                        raise QueueConflictError(f"lease deletion intent identity mismatch: {path}")
                queue_store_write.unlink_durable_path(
                    self._storage_root / "leases" / f"{lease_id}.json",
                    missing_ok=True,
                )
                queue_store_write.unlink_durable_path(
                    self._job_record_path("leases_by_job", job_id, lease_id),
                    missing_ok=True,
                )
                if identity is not None:
                    self._delete_lease_operational_indexes_unlocked(identity)
                capacity_transition = payload.get("lease_capacity_transition")
                if capacity_transition is not None:
                    self._apply_lease_capacity_transition_unlocked(
                        capacity_transition,
                        target="after",
                        label=f"lease deletion {lease_id}",
                    )
                    self._before_lease_capacity_intent_removal("lease_delete", path)
                elif self._lease_capacity_migration_complete_unlocked():
                    raise QueueConflictError(
                        f"lease deletion intent has no capacity transition: {path}"
                    )
                queue_store_write.unlink_durable_path(path, missing_ok=True)
                continue
            if kind == "stale_lease_recovery":
                recovered_stale_jobs.append(
                    self._apply_stale_lease_recovery_intent_unlocked(path, payload)
                )
                continue
            if kind == "job_sync":
                job_id = payload.get("job_id")
                if not isinstance(job_id, str) or not job_id:
                    raise QueueConflictError(f"invalid job transition intent: {path}")
                job = self._read_optional(self._storage_root / "jobs" / f"{job_id}.json", RelayJob)
                if job is not None:
                    self._sync_job_derived_unlocked(job)
                queue_store_write.unlink_durable_path(path, missing_ok=True)
                continue
            if kind == "task_sync":
                task_id = payload.get("task_id")
                if not isinstance(task_id, str) or not task_id:
                    raise QueueConflictError(f"invalid task transition intent: {path}")
                task = self._read_optional(
                    self._storage_root / "tasks" / f"{task_id}.json", RelayTask
                )
                if task is not None:
                    self._sync_task_derived_unlocked(task)
                queue_store_write.unlink_durable_path(path, missing_ok=True)
                continue
            if kind == "gateway_sync":
                session_id = payload.get("session_id")
                if not isinstance(session_id, str) or not session_id:
                    raise QueueConflictError(f"invalid gateway transition intent: {path}")
                self._sync_gateway_session_derived_unlocked(session_id)
                queue_store_write.unlink_durable_path(path, missing_ok=True)
                continue
            raise QueueConflictError(f"unsupported queue transition intent kind {kind!r}: {path}")
        return recovered_stale_jobs

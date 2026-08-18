"""Durable owner-session quiescence and generation-status ownership."""

from __future__ import annotations

from uuid import uuid4

from clio_relay import (
    queue_layout,
    queue_owner_session_records,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError
from clio_relay.models import OwnerSessionClosure, utc_now

_label_key = queue_layout.QueueLayout.label_key


class QueueOwnerSessionLifecycleMixin(queue_owner_session_records.QueueOwnerSessionRecordsMixin):
    """Own owner-session quiescence, cleanup intent, and status behavior."""

    def set_owner_session_closing(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
        operation_id: str | None = None,
        stop_worker: bool = False,
        cancel_jobs: bool = False,
        cancel_scheduler_jobs: bool = False,
    ) -> dict[str, object]:
        """Quiesce one generation and persist its immutable cleanup policy."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        if operation_id is not None and (
            not operation_id.startswith("cleanup_")
            or not queue_layout.safe_global_record_id(operation_id)
        ):
            raise ValueError("operation_id must be a safe cleanup_ identifier")
        if cancel_scheduler_jobs and not cancel_jobs:
            raise ValueError("cancel_scheduler_jobs requires cancel_jobs")
        self._store_adapter.initialize()
        path = (
            self._storage_root
            / "owner_sessions"
            / f"{_label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            existing_closing = self._read_owner_session_transition_record(path)
            existing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                existing_closing,
            )
            active_generation = self._owner_session_active_generation(owner_session_id)
            safely_closed_retry = False
            if active_generation != session_generation_id:
                existing_closure = queue_store_read.read_optional(
                    self._storage_root,
                    self._owner_session_closed_path(
                        owner_session_id,
                        session_generation_id=session_generation_id,
                    ),
                    OwnerSessionClosure,
                )
                safely_closed_retry = (
                    active_generation is None
                    and existing_generation == session_generation_id
                    and existing_closure is not None
                    and existing_closure.owner_session_id == owner_session_id
                    and existing_closure.session_generation_id == session_generation_id
                    and not existing_closure.residual_resource_ids
                )
                if not safely_closed_retry:
                    raise QueueConflictError(
                        f"owner session active generation does not match closing request: "
                        f"{owner_session_id}"
                    )
            if existing_closing is not None and (
                existing_closing.get("owner_session_id") != owner_session_id
                or existing_closing.get("closing") is not True
                or existing_closing.get("session_generation_id") != session_generation_id
            ):
                raise QueueConflictError(
                    f"owner session generation changed before quiescence: {owner_session_id}"
                )
            expected_policy = {
                "stop_worker": stop_worker,
                "cancel_jobs": cancel_jobs,
                "cancel_scheduler_jobs": cancel_scheduler_jobs,
            }
            existing_intent = self._validate_owner_session_cleanup_intent(
                owner_session_id,
                session_generation_id,
                None if existing_closing is None else existing_closing.get("cleanup_intent"),
                required=False,
            )
            if existing_intent is None and safely_closed_retry:
                raise QueueConflictError(
                    "closed owner session has no durable cleanup policy for retry: "
                    f"{owner_session_id}"
                )
            if existing_intent is not None:
                observed_policy = {
                    key: existing_intent[key]
                    for key in (
                        "stop_worker",
                        "cancel_jobs",
                        "cancel_scheduler_jobs",
                    )
                }
                if observed_policy != expected_policy:
                    raise QueueConflictError(
                        f"owner session cleanup policy changed during retry: {owner_session_id}"
                    )
                if operation_id is not None and existing_intent["operation_id"] != operation_id:
                    raise QueueConflictError(
                        f"owner session cleanup operation changed during retry: {owner_session_id}"
                    )
                return existing_intent
            cleanup_intent: dict[str, object] = {
                "schema_version": "clio-relay.owner-session-cleanup-intent.v1",
                "operation_id": operation_id or f"cleanup_{uuid4().hex}",
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                **expected_policy,
                "created_at": utc_now().isoformat(),
            }
            queue_store_write.write_json(
                self._storage_root,
                path,
                {
                    "owner_session_id": owner_session_id,
                    "session_generation_id": session_generation_id,
                    "closing": True,
                    "cleanup_intent": cleanup_intent,
                    "updated_at": utc_now().isoformat(),
                },
            )
            return cleanup_intent

    def get_owner_session_cleanup_intent(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> dict[str, object] | None:
        """Return the immutable cleanup intent for one exact closing generation."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        self._store_adapter.initialize()
        path = (
            self._storage_root
            / "owner_sessions"
            / f"{_label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            closing = self._read_owner_session_transition_record(path)
            closing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                closing,
            )
            if closing_generation is None:
                return None
            if closing_generation != session_generation_id:
                raise QueueConflictError(
                    f"owner session closing generation does not match request: {owner_session_id}"
                )
            return self._validate_owner_session_cleanup_intent(
                owner_session_id,
                session_generation_id,
                closing.get("cleanup_intent") if closing is not None else None,
                required=True,
            )

    def mirror_owner_session_generation_open(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> dict[str, object]:
        """Mirror a remotely verified generation into this queue's admission boundary.

        The caller must verify the authoritative remote session before invoking this
        method. The mirror never reopens the same closed generation and never erases
        an unfinished local cleanup transition.
        """
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        self._store_adapter.initialize()
        active_path = self._owner_session_active_path(owner_session_id)
        closing_path = (
            self._storage_root
            / "owner_sessions"
            / f"{_label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            active = self._read_owner_session_transition_record(active_path)
            closing = self._read_owner_session_transition_record(closing_path)
            active_generation = self._validate_owner_session_active_record(
                owner_session_id,
                active,
            )
            closing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                closing,
            )
            if active_generation not in {None, session_generation_id}:
                raise QueueConflictError(
                    f"owner session active generation does not match remote mirror: "
                    f"{owner_session_id}"
                )
            previous_generation_id: str | None = None
            if closing_generation is not None:
                prior_closure = self.get_owner_session_closed(
                    owner_session_id,
                    session_generation_id=closing_generation,
                )
                safely_closed_prior_generation = (
                    closing_generation != session_generation_id
                    and prior_closure is not None
                    and not prior_closure.residual_resource_ids
                )
                if not safely_closed_prior_generation:
                    raise QueueConflictError(
                        f"owner session has unfinished local cleanup and rejects remote mirror: "
                        f"{owner_session_id}"
                    )
                previous_generation_id = closing_generation
                queue_store_write.unlink_durable_path(closing_path, missing_ok=True)
                closing_generation = None
            if (
                self.get_owner_session_closed(
                    owner_session_id,
                    session_generation_id=session_generation_id,
                )
                is not None
            ):
                raise QueueConflictError(
                    f"owner session generation is already closed: {owner_session_id}"
                )
            if active_generation is None:
                queue_store_write.write_json(
                    self._storage_root,
                    active_path,
                    {
                        "owner_session_id": owner_session_id,
                        "session_generation_id": session_generation_id,
                        "previous_session_generation_id": previous_generation_id,
                        "active": True,
                        "mirrored_remote_authority": True,
                        "updated_at": utc_now().isoformat(),
                    },
                )
            return {
                "schema_version": "clio-relay.owner-session-admission-status.v1",
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                "active_generation_id": session_generation_id,
                "closing_generation_id": closing_generation,
                "active": True,
                "closing": False,
                "closed": False,
                "open": True,
                "cleanup_intent": None,
                "closure": None,
            }

    def owner_session_generation_status(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> dict[str, object]:
        """Return exact machine-readable admission state for one generation."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        self._store_adapter.initialize()
        closing_path = (
            self._storage_root
            / "owner_sessions"
            / f"{_label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            active_generation = self._owner_session_active_generation(owner_session_id)
            closing = self._read_owner_session_transition_record(closing_path)
            closing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                closing,
            )
            cleanup_intent = (
                self._validate_owner_session_cleanup_intent(
                    owner_session_id,
                    session_generation_id,
                    closing.get("cleanup_intent"),
                    required=True,
                )
                if closing_generation == session_generation_id and closing is not None
                else None
            )
            closure = self.get_owner_session_closed(
                owner_session_id,
                session_generation_id=session_generation_id,
            )
            exact_active = active_generation == session_generation_id
            exact_closing = closing_generation == session_generation_id
            closed = closure is not None
            return {
                "schema_version": "clio-relay.owner-session-admission-status.v1",
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                "active_generation_id": active_generation,
                "closing_generation_id": closing_generation,
                "active": exact_active,
                "closing": exact_closing,
                "closed": closed,
                "open": exact_active and closing_generation is None and not closed,
                "cleanup_intent": cleanup_intent,
                "closure": None if closure is None else closure.model_dump(mode="json"),
            }

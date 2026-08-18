"""Canonical gateway-session ownership: submission, state, and writes.

Owns every public method that creates, reads, pages, scans, or transitions a
``GatewaySession``, plus its canonical unlocked write primitive.
``write_gateway_session`` is a module-level function (not only a bound
method) because ``queue_browser_attachments.py``'s CAS transitions must
resolve it through a lookup a test can patch on this module -- design doc
CQ16 row: "Patch each caller owner's collaborator attribute for browser CAS
and backlink synchronization" (the browser-CAS half). The bound method
``_write_gateway_session_unlocked`` stays a real, directly patchable
instance method (``queue_transition_crash_fixture.py`` overrides its sibling
fault hook, ``_after_gateway_canonical_write``, by that exact name) and is a
thin wrapper over the module function, matching the ``queue_jobs.write_job``/
``queue_lease_indexes.sync_operational_indexes`` precedent.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from pydantic import BaseModel

from clio_relay import (
    queue_context,
    queue_gateway_indexes,
    queue_index_state,
    queue_layout,
    queue_owner_session_records,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import GatewaySession, GatewaySessionState, RelayJob, utc_now


def _has_relay_managed_gateway_state(gateway: dict[str, object]) -> bool:
    """Return whether a gateway payload contains relay-owned runtime identity."""
    if {
        "runtime_spec",
        "jarvis_runtime_binding",
        "browser_attachment",
        "ownership_intents",
        "teardown_intent",
        "teardown",
        "detach",
    }.intersection(gateway):
        return True
    transport = gateway.get("transport")
    if not isinstance(transport, dict):
        return False
    return bool(
        {"browser_proxy", "desktop_connector", "remote_connector"}.intersection(
            cast(dict[str, object], transport)
        )
    )


def write_gateway_session(queue: QueueGatewaysMixin, session: GatewaySession) -> None:
    """Write one canonical gateway and replayably converge every backlink."""
    intent_path = queue._write_transition_intent_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "gateway_sync",
        session.session_id,
        {"session_id": session.session_id},
    )
    queue_store_write.write_model(
        queue._storage_root,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._storage_root / "gateway_sessions" / f"{session.session_id}.json",  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session,
    )
    queue._after_gateway_canonical_write(session)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    queue_gateway_indexes.sync_gateway_session_derived(
        cast(queue_gateway_indexes.QueueGatewayIndexesMixin, queue),
        session.session_id,
    )
    queue_store_write.unlink_durable_path(intent_path, missing_ok=True)


class QueueGatewaysMixin:
    """Own canonical gateway-session records: submission, state, and writes."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def _ensure_global_order_entry_unlocked(self, family: str, record_id: str) -> int: ...
        def _assert_owner_session_intake_open_unlocked(
            self, metadata: dict[str, object], *, require_active: bool = False
        ) -> None: ...
        def _write_transition_intent_unlocked(
            self, kind: str, identity: str, payload: dict[str, object]
        ) -> Path: ...
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

    def create_gateway_session(self, session: GatewaySession) -> GatewaySession:
        """Create a durable scheduler-backed gateway session record."""
        queue_layout.QueueLayout.require_durable_record_id(session.session_id, field="session_id")
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            self._recover_pending_transitions_unlocked()
            existing = queue_store_read.read_optional(
                self._storage_root,
                self._storage_root / "gateway_sessions" / f"{session.session_id}.json",
                GatewaySession,
            )
            if existing is not None:
                if existing.session_id != session.session_id:
                    raise QueueConflictError(
                        f"canonical gateway session identity mismatch: {session.session_id}"
                    )
                raise QueueConflictError(f"gateway session already exists: {session.session_id}")
            queue_owner_session_records._validate_owner_session_identity_metadata(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                session.metadata,
                allow_legacy=False,
            )
            self._assert_owner_session_intake_open_unlocked(
                session.metadata,
                require_active=True,
            )
            self._ensure_global_order_entry_unlocked(
                "gateway_sessions",
                session.session_id,
            )
            self._write_gateway_session_unlocked(session)
        return session

    def get_gateway_session(self, session_id: str) -> GatewaySession:
        """Return a gateway session by id."""
        session_id = queue_layout.QueueLayout.require_durable_record_id(
            session_id, field="session_id"
        )
        session = queue_store_read.read_optional(
            self._storage_root,
            self._storage_root / "gateway_sessions" / f"{session_id}.json",
            GatewaySession,
        )
        if session is None:
            raise NotFoundError(f"gateway session not found: {session_id}")
        if session.session_id != session_id:
            raise QueueConflictError(f"canonical gateway session identity mismatch: {session_id}")
        return session

    def list_gateway_sessions(self, cluster: str | None = None) -> list[GatewaySession]:
        """Return durable gateway sessions, optionally filtered by cluster."""
        self._store_adapter.initialize()
        sessions = list(
            queue_store_read.read_many(
                self._storage_root / "gateway_sessions",
                GatewaySession,
                identity_field="session_id",
            )
        )
        if cluster is not None:
            sessions = [session for session in sessions if session.cluster == cluster]
        return sorted(sessions, key=lambda session: session.created_at)

    def list_gateway_sessions_page(
        self,
        *,
        cursor: int = 1,
        limit: int = 100,
        cluster: str | None = None,
        state: GatewaySessionState | None = None,
    ) -> tuple[list[GatewaySession], int | None, int]:
        """Read one global gateway-session source window with in-window filters."""

        def matches(session: GatewaySession) -> bool:
            return (cluster is None or session.cluster == cluster) and (
                state is None or session.state == state
            )

        return self._read_global_order_page(
            family="gateway_sessions",
            model=GatewaySession,
            identity_field="session_id",
            cursor=cursor,
            limit=limit,
            predicate=matches,
        )

    def scan_gateway_sessions(
        self,
        *,
        limit: int,
        cluster: str | None = None,
        state: GatewaySessionState | None = None,
    ) -> tuple[list[GatewaySession], bool]:
        """Read one bounded gateway-session source window and truncation state."""

        def matches(session: GatewaySession) -> bool:
            return (cluster is None or session.cluster == cluster) and (
                state is None or session.state == state
            )

        return self._scan_global_order(
            family="gateway_sessions",
            model=GatewaySession,
            identity_field="session_id",
            limit=limit,
            predicate=matches,
        )

    def update_gateway_session(
        self,
        session_id: str,
        *,
        state: GatewaySessionState | None = None,
        metadata: dict[str, object] | None = None,
        expected_updated_at: object = None,
        allow_owned_runtime_close: object = False,
        reject_relay_managed_fields: object = False,
        **updates: object,
    ) -> GatewaySession:
        """Merge gateway state using an optional optimistic transition guard."""
        session_id = queue_layout.QueueLayout.require_durable_record_id(
            session_id, field="session_id"
        )
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            if expected_updated_at is not None and not isinstance(expected_updated_at, datetime):
                raise ValueError("expected_updated_at must be an aware datetime")
            if expected_updated_at is not None and session.updated_at != expected_updated_at:
                raise QueueConflictError(
                    f"gateway session changed during a runtime transition: {session_id}"
                )
            self._ensure_global_order_entry_unlocked(
                "gateway_sessions",
                session.session_id,
            )
            is_owned_runtime = session.metadata.get("owner") == "clio-relay" and isinstance(
                session.gateway.get("runtime_spec"), dict
            )
            if (
                reject_relay_managed_fields is True
                and "gateway" in updates
                and _has_relay_managed_gateway_state(session.gateway)
            ):
                raise QueueConflictError(
                    "generic gateway updates cannot replace relay-managed runtime state: "
                    f"{session_id}"
                )
            if (
                state == GatewaySessionState.CLOSED
                and session.state != GatewaySessionState.CLOSED
                and is_owned_runtime
                and allow_owned_runtime_close is not True
            ):
                raise QueueConflictError(
                    "owned runtime gateway sessions must be closed with stop-runtime so "
                    "connectors are proven stopped first"
                )
            if session.state == GatewaySessionState.CLOSED:
                if state is not None and state != GatewaySessionState.CLOSED:
                    raise QueueConflictError(f"cannot reopen closed gateway session: {session_id}")
                if updates and allow_owned_runtime_close is not True:
                    raise QueueConflictError(f"cannot update closed gateway session: {session_id}")
            current_teardown_intent = session.gateway.get("teardown_intent")
            if current_teardown_intent is not None and "gateway" in updates:
                replacement_gateway = updates.get("gateway")
                if (
                    not isinstance(replacement_gateway, dict)
                    or cast(dict[str, object], replacement_gateway).get("teardown_intent")
                    != current_teardown_intent
                ):
                    raise QueueConflictError(
                        "a committed gateway teardown intent cannot be removed or changed: "
                        f"{session_id}"
                    )
            merged_metadata = dict(session.metadata)
            if metadata:
                merged_metadata.update(metadata)
            payload = dict(updates)
            if state is not None:
                payload["state"] = state
            payload["metadata"] = merged_metadata
            payload["updated_at"] = utc_now()
            updated = session.model_copy(update=payload)
            self._write_gateway_session_unlocked(updated)
            return updated

    def prepare_gateway_teardown_intent(
        self,
        session_id: str,
        *,
        cancel_scheduler_job: bool,
    ) -> GatewaySession:
        """Atomically create or validate one immutable gateway cleanup policy."""
        session_id = queue_layout.QueueLayout.require_durable_record_id(
            session_id, field="session_id"
        )
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            raw_intent = session.gateway.get("teardown_intent")
            if raw_intent is not None:
                if not isinstance(raw_intent, dict):
                    raise QueueConflictError("gateway teardown intent is invalid")
                intent = cast(dict[str, object], raw_intent)
                operation_id = intent.get("operation_id")
                created_at = intent.get("created_at")
                if (
                    intent.get("schema_version") != "clio-relay.gateway-teardown-intent.v1"
                    or intent.get("gateway_session_id") != session_id
                    or not isinstance(operation_id, str)
                    or not operation_id.startswith("gateway_cleanup_")
                    or not queue_layout.safe_global_record_id(operation_id)
                    or not isinstance(created_at, str)
                    or not isinstance(intent.get("cancel_scheduler_job"), bool)
                ):
                    raise QueueConflictError("gateway teardown intent is invalid")
                try:
                    parsed_created_at = datetime.fromisoformat(created_at)
                except ValueError as exc:
                    raise QueueConflictError("gateway teardown intent time is invalid") from exc
                if parsed_created_at.tzinfo is None:
                    raise QueueConflictError("gateway teardown intent time is naive")
                if intent.get("cancel_scheduler_job") is not cancel_scheduler_job:
                    raise QueueConflictError(
                        "gateway cleanup policy changed during retry; resume with the original "
                        f"cancel_scheduler_job={intent.get('cancel_scheduler_job')} policy"
                    )
                return session
            if session.state == GatewaySessionState.CLOSED:
                raise QueueConflictError(
                    f"closed gateway session has no durable teardown intent: {session_id}"
                )
            gateway = {
                **session.gateway,
                "teardown_intent": {
                    "schema_version": "clio-relay.gateway-teardown-intent.v1",
                    "operation_id": f"gateway_cleanup_{uuid4().hex}",
                    "gateway_session_id": session_id,
                    "cancel_scheduler_job": cancel_scheduler_job,
                    "created_at": utc_now().isoformat(),
                },
            }
            updated = session.model_copy(update={"gateway": gateway, "updated_at": utc_now()})
            self._write_gateway_session_unlocked(updated)
            return updated

    def close_gateway_session(self, session_id: str) -> GatewaySession:
        """Mark a gateway session closed."""
        session_id = queue_layout.QueueLayout.require_durable_record_id(
            session_id, field="session_id"
        )
        return self.update_gateway_session(session_id, state=GatewaySessionState.CLOSED)

    def _write_gateway_session_unlocked(self, session: GatewaySession) -> None:
        """Write one canonical gateway and replayably converge every backlink."""
        write_gateway_session(self, session)

    def _after_gateway_canonical_write(self, _session: GatewaySession) -> None:
        """Fault-injection seam after a canonical gateway transition."""

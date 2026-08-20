"""Browser-attachment CAS ownership: the one-slot ownership-intent lifecycle.

Owns every public method that reserves, publishes, revokes, or finishes
revoking the sole browser attachment a gateway session may hold, plus the
identity/ownership-intent validation those CAS transitions share. Every
canonical write resolves through ``queue_gateways.write_gateway_session``,
the collaborator-attribute lookup a test can patch on this module -- design
doc CQ16 row: "Patch each caller owner's collaborator attribute for browser
CAS and backlink synchronization" (the browser-CAS half).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from clio_relay import queue_context, queue_gateways, queue_index_state, queue_layout
from clio_relay.browser_gateway import BrowserAttachmentRecord
from clio_relay.errors import BrowserAttachmentIdentityConflictError, QueueConflictError
from clio_relay.models import GatewaySession, GatewaySessionState, RelayJob, utc_now


def _require_browser_attachment_session_ready(session: GatewaySession) -> None:
    """Require the latest gateway state to remain eligible for a new attachment."""
    if session.metadata.get("owner") != "clio-relay":
        raise QueueConflictError("browser attachment requires an owned clio-relay runtime")
    if session.state is not GatewaySessionState.READY:
        raise QueueConflictError("browser attachment requires a ready gateway session")
    if session.gateway.get("teardown_intent") is not None:
        raise QueueConflictError("a gateway committed to teardown cannot issue attachments")
    if not isinstance(session.gateway.get("jarvis_runtime_binding"), dict):
        raise QueueConflictError("browser attachment requires a verified JARVIS binding")
    if not isinstance(session.gateway.get("runtime_spec"), dict):
        raise QueueConflictError("browser attachment requires an owned runtime specification")


def _browser_attachment_record(
    session: GatewaySession,
    *,
    required: bool,
) -> BrowserAttachmentRecord | None:
    """Parse the exact current browser attachment below one gateway session."""
    raw = session.gateway.get("browser_attachment")
    if raw is None:
        if required:
            raise QueueConflictError("gateway has no browser attachment")
        return None
    try:
        return BrowserAttachmentRecord.model_validate(raw)
    except ValueError as exc:
        raise QueueConflictError("gateway browser attachment record is invalid") from exc


def _gateway_mapping(gateway: dict[str, object], field: str) -> dict[str, object]:
    """Return a copy of one optional gateway mapping or fail on corrupt state."""
    raw = gateway.get(field)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise QueueConflictError(f"gateway {field} record is invalid")
    return dict(cast(dict[str, object], raw))


def _validate_browser_proxy_intent(
    intent: dict[str, object],
    *,
    attachment_id: str,
    expected_state: str | None = None,
) -> None:
    """Bind one ownership transition to the exact browser attachment."""
    state = intent.get("state")
    if (
        intent.get("schema_version") != "clio-relay.gateway-ownership-intent.v1"
        or intent.get("attachment_id") != attachment_id
        or not isinstance(state, str)
        or (expected_state is not None and state != expected_state)
    ):
        raise QueueConflictError("browser proxy ownership intent identity is invalid")
    for field in ("owner_token", "connector_generation_id", "config_path"):
        value = intent.get(field)
        if not isinstance(value, str) or not value:
            raise QueueConflictError(f"browser proxy ownership intent has no {field}")


def _validate_browser_proxy_identity(
    proxy: dict[str, object],
    *,
    attachment_id: str,
    proxy_process_id: int | None,
) -> None:
    """Require one process record to belong to the exact attachment transition."""
    if proxy.get("attachment_id") != attachment_id:
        raise QueueConflictError("browser proxy attachment identity is invalid")
    pid = proxy.get("pid")
    if (
        proxy_process_id is None
        or not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid != proxy_process_id
    ):
        raise QueueConflictError("browser proxy process identity is invalid")


def _require_browser_proxy_ownership_consistent(
    *documents: dict[str, object],
) -> None:
    """Require every transition document to retain the same bearer ownership identity."""
    for field in ("owner_token", "connector_generation_id", "config_path"):
        values = [document.get(field) for document in documents]
        if (
            not all(isinstance(value, str) and value for value in values)
            or len(set(cast(list[str], values))) != 1
        ):
            raise QueueConflictError(f"browser proxy {field} changed during transition")


def _require_same_browser_attachment(
    current: BrowserAttachmentRecord,
    proposed: BrowserAttachmentRecord,
) -> None:
    """Reject changes to capability identity across lifecycle-only transitions."""
    excluded = {"state", "proxy_process_id", "revoked_at"}
    if current.model_dump(exclude=excluded) != proposed.model_dump(exclude=excluded):
        raise QueueConflictError("browser attachment identity changed during transition")
    if (
        current.proxy_process_id is not None
        and proposed.proxy_process_id != current.proxy_process_id
    ):
        raise QueueConflictError("browser attachment proxy process changed during transition")


class QueueBrowserAttachmentsMixin:
    """Own the sole browser-attachment ownership-intent CAS lifecycle."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def get_gateway_session(self, session_id: str) -> GatewaySession: ...

    def prepare_gateway_browser_attachment(
        self,
        session_id: str,
        *,
        attachment: BrowserAttachmentRecord,
        browser_proxy_intent: dict[str, object],
    ) -> GatewaySession:
        """Atomically reserve the sole browser attachment slot for one exact identity."""
        session_id = queue_layout.QueueLayout.require_durable_record_id(
            session_id, field="session_id"
        )
        if attachment.state != "starting":
            raise ValueError("prepared browser attachment must be in starting state")
        _validate_browser_proxy_intent(
            browser_proxy_intent,
            attachment_id=attachment.attachment_id,
            expected_state="starting",
        )
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            _require_browser_attachment_session_ready(session)
            existing = _browser_attachment_record(session, required=False)
            if existing is not None and existing.state != "revoked":
                raise QueueConflictError(
                    "gateway already has a browser attachment transition: "
                    f"{existing.attachment_id} ({existing.state})"
                )
            if existing is not None and existing.attachment_id == attachment.attachment_id:
                raise QueueConflictError("revoked browser attachment identities cannot be reused")
            transport = _gateway_mapping(session.gateway, "transport")
            if transport.get("browser_proxy") is not None:
                raise QueueConflictError("gateway has a browser proxy without an active slot")
            intents = _gateway_mapping(session.gateway, "ownership_intents")
            current_intent = intents.get("browser_proxy")
            if isinstance(current_intent, dict) and cast(dict[str, object], current_intent).get(
                "state"
            ) not in {"not_started", "absent_verified"}:
                raise QueueConflictError("gateway browser proxy ownership is not absent")
            intents["browser_proxy"] = dict(browser_proxy_intent)
            gateway = {
                **session.gateway,
                "browser_attachment": attachment.model_dump(mode="json"),
                "ownership_intents": intents,
            }
            return self._write_browser_attachment_transition_unlocked(
                session,
                gateway=gateway,
            )

    def complete_gateway_browser_attachment(
        self,
        session_id: str,
        *,
        attachment: BrowserAttachmentRecord,
        browser_proxy: dict[str, object],
        browser_proxy_intent: dict[str, object],
    ) -> GatewaySession:
        """Atomically publish one started proxy without overwriting newer gateway state."""
        session_id = queue_layout.QueueLayout.require_durable_record_id(
            session_id, field="session_id"
        )
        if attachment.state != "active" or attachment.proxy_process_id is None:
            raise ValueError("completed browser attachment must be active with a proxy pid")
        _validate_browser_proxy_identity(
            browser_proxy,
            attachment_id=attachment.attachment_id,
            proxy_process_id=attachment.proxy_process_id,
        )
        _validate_browser_proxy_intent(
            browser_proxy_intent,
            attachment_id=attachment.attachment_id,
            expected_state="recorded",
        )
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            _require_browser_attachment_session_ready(session)
            current = _browser_attachment_record(session, required=True)
            assert current is not None
            if current.state == "active" and current == attachment:
                return session
            if current.state != "starting":
                raise QueueConflictError(
                    "browser attachment cannot complete from "
                    f"{current.state}: {current.attachment_id}"
                )
            _require_same_browser_attachment(current, attachment)
            intents = _gateway_mapping(session.gateway, "ownership_intents")
            current_intent = intents.get("browser_proxy")
            if not isinstance(current_intent, dict):
                raise QueueConflictError("browser attachment has no starting ownership intent")
            _validate_browser_proxy_intent(
                cast(dict[str, object], current_intent),
                attachment_id=attachment.attachment_id,
                expected_state="starting",
            )
            _require_browser_proxy_ownership_consistent(
                cast(dict[str, object], current_intent),
                browser_proxy,
                browser_proxy_intent,
            )
            intents["browser_proxy"] = dict(browser_proxy_intent)
            transport = _gateway_mapping(session.gateway, "transport")
            if transport.get("browser_proxy") is not None:
                raise QueueConflictError("browser attachment proxy was already published")
            transport["browser_proxy"] = dict(browser_proxy)
            gateway = {
                **session.gateway,
                "browser_attachment": attachment.model_dump(mode="json"),
                "ownership_intents": intents,
                "transport": transport,
            }
            return self._write_browser_attachment_transition_unlocked(
                session,
                gateway=gateway,
            )

    def begin_gateway_browser_attachment_revoke(
        self,
        session_id: str,
        *,
        attachment_id: str,
    ) -> GatewaySession:
        """Atomically move the exact current attachment into revocation."""
        session_id = queue_layout.QueueLayout.require_durable_record_id(
            session_id, field="session_id"
        )
        if not attachment_id:
            raise ValueError("attachment_id must not be empty")
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            current = _browser_attachment_record(session, required=True)
            assert current is not None
            if current.attachment_id != attachment_id:
                raise BrowserAttachmentIdentityConflictError(
                    "browser attachment changed before revocation: "
                    f"{current.attachment_id} != {attachment_id}"
                )
            if current.state in {"revoking", "revoked"}:
                return session
            revoking = current.model_copy(update={"state": "revoking"})
            gateway = {
                **session.gateway,
                "browser_attachment": revoking.model_dump(mode="json"),
            }
            return self._write_browser_attachment_transition_unlocked(
                session,
                gateway=gateway,
            )

    def finish_gateway_browser_attachment_revoke(
        self,
        session_id: str,
        *,
        attachment: BrowserAttachmentRecord,
        browser_proxy_absent_intent: dict[str, object] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> GatewaySession:
        """Atomically finish exact revocation while retaining concurrent teardown state."""
        session_id = queue_layout.QueueLayout.require_durable_record_id(
            session_id, field="session_id"
        )
        if attachment.state not in {"revoked", "failed"}:
            raise ValueError("finished browser attachment must be revoked or failed")
        if attachment.state == "revoked":
            if browser_proxy_absent_intent is None:
                raise ValueError("revoked browser attachment requires an absent proxy intent")
            _validate_browser_proxy_intent(
                browser_proxy_absent_intent,
                attachment_id=attachment.attachment_id,
                expected_state="absent_verified",
            )
        elif browser_proxy_absent_intent is not None:
            raise ValueError("failed browser attachment cannot claim proxy absence")
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            current = _browser_attachment_record(session, required=True)
            assert current is not None
            if current.attachment_id != attachment.attachment_id:
                raise BrowserAttachmentIdentityConflictError(
                    "browser attachment changed before revocation completed: "
                    f"{current.attachment_id} != {attachment.attachment_id}"
                )
            if current.state == "revoked":
                _require_same_browser_attachment(current, attachment)
                return session
            if current.state == "failed" and attachment.state == "failed":
                _require_same_browser_attachment(current, attachment)
                return session
            if current.state not in {"revoking", "failed"} or (
                current.state == "failed" and attachment.state != "revoked"
            ):
                raise QueueConflictError(
                    "browser attachment cannot finish revocation from "
                    f"{current.state}: {current.attachment_id}"
                )
            _require_same_browser_attachment(current, attachment)
            gateway = dict(session.gateway)
            gateway["browser_attachment"] = attachment.model_dump(mode="json")
            if attachment.state == "revoked":
                assert browser_proxy_absent_intent is not None
                transport = _gateway_mapping(session.gateway, "transport")
                current_proxy = transport.get("browser_proxy")
                if isinstance(current_proxy, dict):
                    _validate_browser_proxy_identity(
                        cast(dict[str, object], current_proxy),
                        attachment_id=attachment.attachment_id,
                        proxy_process_id=attachment.proxy_process_id,
                    )
                transport.pop("browser_proxy", None)
                intents = _gateway_mapping(session.gateway, "ownership_intents")
                current_intent = intents.get("browser_proxy")
                if isinstance(current_intent, dict):
                    _validate_browser_proxy_intent(
                        cast(dict[str, object], current_intent),
                        attachment_id=attachment.attachment_id,
                    )
                    _require_browser_proxy_ownership_consistent(
                        cast(dict[str, object], current_intent),
                        browser_proxy_absent_intent,
                    )
                intents["browser_proxy"] = dict(browser_proxy_absent_intent)
                gateway["transport"] = transport
                gateway["ownership_intents"] = intents
            return self._write_browser_attachment_transition_unlocked(
                session,
                gateway=gateway,
                metadata=metadata,
            )

    def _write_browser_attachment_transition_unlocked(
        self,
        session: GatewaySession,
        *,
        gateway: dict[str, Any],
        metadata: dict[str, object] | None = None,
    ) -> GatewaySession:
        """Persist one lock-held attachment transition against the latest session."""
        merged_metadata = dict(session.metadata)
        if metadata:
            merged_metadata.update(metadata)
        updated = session.model_copy(
            update={
                "gateway": gateway,
                "metadata": merged_metadata,
                "updated_at": utc_now(),
            }
        )
        queue_gateways.write_gateway_session(
            cast(queue_gateways.QueueGatewaysMixin, self),
            updated,
        )
        return updated

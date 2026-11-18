"""Browser attachment cleanup for ``ServiceRuntimeSupervisor``.

Relay no longer issues browser capabilities: a served service's secret-bearing
URLs reach the client only through the serve result's ``_meta``. A gateway
record written by an older relay may still hold an active browser attachment
and its loopback proxy, so detach and teardown still revoke it.
``_revoke_browser_attachment`` is idempotent on an already-revoked record and
proves the proxy is gone before marking its ownership intent
``absent_verified``. ``_revoke_browser_for_runtime_cleanup`` is the
best-effort variant, so an invalid or already-revoked record never blocks
runtime cleanup.

This is a mixin: it depends on ``self.queue`` and ``self.settings`` from
``_ServiceRuntimeCoreMixin.__init__`` and on ``self._stop_local_connector``.
"""

from __future__ import annotations

from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_readiness as _readiness
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay.adapters.worker.browser_attachment_record import BrowserAttachmentRecord
from clio_relay.errors import BrowserAttachmentIdentityConflictError, ConfigurationError, RelayError
from clio_relay.models import GatewaySession, utc_now
from clio_relay.session_wire_models import CleanupResource


class _ServiceRuntimeBrowserMixin:
    """Revoke a browser attachment left by an older relay."""

    def _revoke_browser_attachment(
        self,
        session: GatewaySession,
        *,
        attachment_id: str,
    ) -> tuple[GatewaySession, CleanupResource]:
        try:
            session = self.queue.begin_gateway_browser_attachment_revoke(
                session.session_id,
                attachment_id=attachment_id,
            )
        except BrowserAttachmentIdentityConflictError as exc:
            raise ConfigurationError(
                "browser attachment id does not match the gateway record"
            ) from exc
        raw_record = session.gateway.get("browser_attachment")
        try:
            record = BrowserAttachmentRecord.model_validate(raw_record)
        except ValueError as exc:
            raise RelayError("gateway contains an invalid browser attachment record") from exc
        if record.state == "revoked":
            return (
                session,
                CleanupResource(
                    kind="browser_proxy",
                    resource_id=str(record.proxy_process_id or record.attachment_id),
                    location="desktop",
                    action="stop",
                    ownership_verified=True,
                    outcome="missing",
                    verified_after_operation=True,
                    metadata={"gateway_session_id": session.session_id},
                ),
            )
        revocation_path = _readiness._owned_browser_runtime_path(
            self.settings,
            session.session_id,
            record.revocation_path,
        )
        _readiness._write_browser_revocation_marker(revocation_path, record.attachment_id)
        transport = _primitives._object(session.gateway.get("transport", {}))
        proxy = _primitives._object(transport.get("browser_proxy", {}))
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get("browser_proxy", {}))
        absence_verified = False
        if not proxy:
            proxy, absence_verified = _connector_identity._discover_local_connector(
                intent,
                session_id=session.session_id,
            )
            proxy = proxy or {}
        _stopped_pid, cleanup = self._stop_local_connector(
            session_id=session.session_id,
            connector=proxy,
            require_record=True,
            absence_verified=absence_verified,
        )
        cleanup = cleanup.model_copy(
            update={
                "kind": "browser_proxy",
                "metadata": {
                    **cleanup.metadata,
                    "gateway_session_id": session.session_id,
                    "attachment_id": attachment_id,
                },
            }
        )
        revoked_at = utc_now().isoformat()
        if cleanup.residual:
            failed = record.model_copy(update={"state": "failed"})
            session = self.queue.finish_gateway_browser_attachment_revoke(
                session.session_id,
                attachment=failed,
                metadata={"browser_detach_error": cleanup.detail},
            )
            return session, cleanup
        revoked = record.model_copy(update={"state": "revoked", "revoked_at": revoked_at})
        intents["browser_proxy"] = _scheduler_contracts._new_ownership_intent(
            "absent_verified",
            attachment_id=attachment_id,
            owner_token=intent.get("owner_token"),
            connector_generation_id=intent.get("connector_generation_id"),
            config_path=intent.get("config_path"),
        )
        session = self.queue.finish_gateway_browser_attachment_revoke(
            session.session_id,
            attachment=revoked,
            browser_proxy_absent_intent=_primitives._object(intents["browser_proxy"]),
            metadata={"browser_detached_at": revoked_at},
        )
        return session, cleanup

    def _revoke_browser_for_runtime_cleanup(
        self,
        session: GatewaySession,
    ) -> tuple[GatewaySession, CleanupResource | None, str | None]:
        """Revoke any active browser attachment as part of detach or teardown."""
        raw_record = session.gateway.get("browser_attachment")
        if raw_record is None:
            return session, None, None
        try:
            record = BrowserAttachmentRecord.model_validate(raw_record)
        except ValueError as exc:
            detail = f"browser attachment record is invalid: {exc}"
            return (
                session,
                CleanupResource(
                    kind="browser_proxy",
                    resource_id=session.session_id,
                    location="desktop",
                    action="stop",
                    ownership_verified=False,
                    outcome="refused",
                    residual=True,
                    detail=detail,
                    metadata={"gateway_session_id": session.session_id},
                ),
                detail,
            )
        if record.state == "revoked":
            return session, None, None
        try:
            session, cleanup = self._revoke_browser_attachment(
                session,
                attachment_id=record.attachment_id,
            )
        except (ConfigurationError, RelayError) as exc:
            detail = str(exc)
            return (
                session,
                CleanupResource(
                    kind="browser_proxy",
                    resource_id=str(record.proxy_process_id or record.attachment_id),
                    location="desktop",
                    action="stop",
                    ownership_verified=False,
                    outcome="failed",
                    residual=True,
                    detail=detail,
                    metadata={"gateway_session_id": session.session_id},
                ),
                detail,
            )
        return session, cleanup, cleanup.detail if cleanup.residual else None

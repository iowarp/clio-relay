"""Browser sandbox attachment/detachment for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 13, class-mixin
split): the public ``browser_attach``/``browser_detach`` entry points and
their transition-lock-serialized implementations, the shared
``_revoke_browser_attachment`` revocation (idempotent on an already-revoked
record, proves loopback proxy absence before marking the ownership intent
``absent_verified``), and ``_revoke_browser_for_runtime_cleanup`` -- the
best-effort variant detach/teardown call so an invalid or already-revoked
attachment record never blocks runtime cleanup.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.queue``, ``self.settings``,
``self.definition``, ``self.cluster``) and calls back into sibling mixins
through ``self`` -- JARVIS authorization
(``self._jarvis_runtime_authorization``), the loopback proxy process
lifecycle (``self._start_browser_proxy``, ``self._stop_local_connector``,
``self._wait_for_browser_health``). Python's MRO resolves every one of
those through whichever mixin defines it regardless of call origin, so no
cross-mixin qualification is used. The class docstring in
``service_runtime.py`` records the full mixin composition.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import timedelta
from pathlib import Path
from typing import cast

from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_readiness as _readiness
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay.browser_gateway import (
    BrowserAttachmentGrant,
    BrowserAttachmentRecord,
    BrowserDetachmentResult,
    BrowserGatewayConfig,
)
from clio_relay.errors import BrowserAttachmentIdentityConflictError, ConfigurationError, RelayError
from clio_relay.jarvis_service_runtime import reverify_jarvis_service_runtime
from clio_relay.models import GatewaySession, GatewaySessionState, ServiceRuntimeSpec, utc_now
from clio_relay.session_wire_models import CleanupResource


class _ServiceRuntimeBrowserMixin:
    """Issue and revoke short-lived browser sandbox capabilities."""

    def browser_attach(
        self,
        *,
        session_id: str,
        ttl_seconds: int = 1_800,
        bind_port: int | None = None,
    ) -> BrowserAttachmentGrant:
        """Serialize browser capability creation against all gateway transitions."""
        with self._gateway_transition_lock(session_id):
            return self._browser_attach_serialized(
                session_id=session_id,
                ttl_seconds=ttl_seconds,
                bind_port=bind_port,
            )

    def _browser_attach_serialized(
        self,
        *,
        session_id: str,
        ttl_seconds: int = 1_800,
        bind_port: int | None = None,
    ) -> BrowserAttachmentGrant:
        """Issue one short-lived sandbox capability through an owned loopback proxy."""
        if ttl_seconds < 60 or ttl_seconds > 28_800:
            raise ConfigurationError("browser attachment TTL must be between 60 and 28800 seconds")
        session = self.queue.get_gateway_session(session_id)
        if session.cluster != self.cluster:
            raise ConfigurationError(
                f"gateway session {session_id} belongs to cluster {session.cluster}, "
                f"not {self.cluster}"
            )
        if session.metadata.get("owner") != "clio-relay":
            raise ConfigurationError("browser attachment requires an owned clio-relay runtime")
        if session.state is not GatewaySessionState.READY:
            raise ConfigurationError("browser attachment requires a ready gateway session")
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError("a gateway committed to teardown cannot issue attachments")
        binding_document = session.gateway.get("jarvis_runtime_binding")
        if binding_document is None:
            raise ConfigurationError("browser attachment requires a verified JARVIS binding")
        try:
            verified_runtime = reverify_jarvis_service_runtime(
                queue=self.queue,
                definition=self.definition,
                settings=self.settings,
                binding_document=binding_document,
            )
        except ValueError as exc:
            raise RelayError(
                f"JARVIS service runtime binding re-verification failed: {exc}"
            ) from exc
        try:
            spec = ServiceRuntimeSpec.model_validate(session.gateway.get("runtime_spec"))
        except ValueError as exc:
            raise RelayError("owned runtime has no valid service runtime specification") from exc
        if spec.deployment_driver != "jarvis-bound" or spec.command_path is None:
            raise ConfigurationError("browser attachment requires a JARVIS-bound command contract")
        existing_document = session.gateway.get("browser_attachment")
        if existing_document is not None:
            try:
                existing = BrowserAttachmentRecord.model_validate(existing_document)
            except ValueError as exc:
                raise RelayError("gateway contains an invalid browser attachment record") from exc
            if existing.state != "revoked":
                expiry = _readiness._utc_timestamp(existing.expires_at)
                if expiry > utc_now() and not Path(existing.revocation_path).exists():
                    raise ConfigurationError(
                        "gateway already has an active browser attachment; "
                        "detach it before rotating"
                    )
                session, _result, cleanup = self._revoke_browser_attachment(
                    session,
                    attachment_id=existing.attachment_id,
                )
                if cleanup.residual:
                    raise RelayError(cleanup.detail or "expired browser proxy cleanup failed")

        public_port = bind_port or _readiness._available_loopback_port(
            exclude={spec.desktop_bind_port}
        )
        if public_port < 1 or public_port > 65_535:
            raise ConfigurationError("browser attachment bind port must be between 1 and 65535")
        if public_port == spec.desktop_bind_port:
            raise ConfigurationError("browser attachment port must differ from the direct port")
        attachment_id = f"browser-{secrets.token_hex(16)}"
        capability = secrets.token_urlsafe(32)
        issued_at = utc_now()
        expires_at = issued_at + timedelta(seconds=ttl_seconds)
        runtime_dir = (
            self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        ).resolve()
        runtime_dir.mkdir(parents=True, exist_ok=True)
        config_path = runtime_dir / f"{attachment_id}.browser-gateway.json"
        revocation_path = runtime_dir / f"{attachment_id}.revoked"
        stdout_path = runtime_dir / f"{attachment_id}.browser-gateway.out"
        stderr_path = runtime_dir / f"{attachment_id}.browser-gateway.err"
        metadata_path = runtime_dir / f"{attachment_id}.browser-gateway-owner.json"
        token_sha256 = hashlib.sha256(capability.encode("utf-8")).hexdigest()
        paths = list(
            dict.fromkeys(
                [
                    "/",
                    spec.health_path,
                    spec.stream_path,
                    spec.event_stream_path,
                    spec.state_path,
                    spec.command_path,
                ]
            )
        )
        if any(path is None for path in paths):
            raise ConfigurationError("JARVIS browser attachment requires all six endpoint paths")
        config = BrowserGatewayConfig(
            attachment_id=attachment_id,
            token_sha256=token_sha256,
            bind_port=public_port,
            upstream_protocol=spec.protocol,
            upstream_port=spec.desktop_bind_port,
            allowed_paths=cast(list[str], paths),
            command_path=spec.command_path,
            expires_at=expires_at.isoformat(),
            revocation_path=str(revocation_path),
        )
        intent = _scheduler_contracts._new_ownership_intent(
            "starting",
            owner_token=secrets.token_hex(32),
            connector_generation_id=secrets.token_hex(16),
            config_path=str(config_path),
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            metadata_path=str(metadata_path),
            attachment_id=attachment_id,
        )
        record = BrowserAttachmentRecord(
            attachment_id=attachment_id,
            state="starting",
            issued_at=issued_at.isoformat(),
            expires_at=expires_at.isoformat(),
            token_sha256=token_sha256,
            bind_port=public_port,
            revocation_path=str(revocation_path),
        )
        session = self.queue.prepare_gateway_browser_attachment(
            session.session_id,
            attachment=record,
            browser_proxy_intent=intent,
        )
        proxy: dict[str, object] | None = None
        try:
            proxy = self._start_browser_proxy(
                session=session,
                config=config,
                capability=capability,
                upstream_authorization=self._jarvis_runtime_authorization(verified_runtime),
                ownership_intent=intent,
            )
            active = record.model_copy(update={"state": "active", "proxy_process_id": proxy["pid"]})
            session = self.queue.complete_gateway_browser_attachment(
                session.session_id,
                attachment=active,
                browser_proxy=proxy,
                browser_proxy_intent=_scheduler_contracts._new_ownership_intent(
                    "recorded", **proxy
                ),
            )
            grant = _readiness._browser_attachment_grant(
                record=active,
                capability=capability,
                spec=spec,
            )
            self._wait_for_browser_health(
                grant.health_url,
                timeout_seconds=min(spec.readiness_timeout_seconds, 60.0),
                poll_seconds=min(spec.poll_seconds, 1.0),
            )
            return grant
        except Exception as exc:
            cleanup_detail: str | None = None
            try:
                latest = self.queue.get_gateway_session(session.session_id)
                _latest, _result, cleanup = self._revoke_browser_attachment(
                    latest,
                    attachment_id=attachment_id,
                )
                if cleanup.residual:
                    cleanup_detail = cleanup.detail
            except RelayError as cleanup_exc:
                cleanup_detail = str(cleanup_exc)
            if proxy is not None:
                _stopped_pid, direct_cleanup = self._stop_local_connector(
                    session_id=session.session_id,
                    connector=proxy,
                    require_record=True,
                )
                if direct_cleanup.residual:
                    cleanup_detail = direct_cleanup.detail or cleanup_detail
            if cleanup_detail is not None:
                latest = self.queue.get_gateway_session(session.session_id)
                self.queue.update_gateway_session(
                    latest.session_id,
                    metadata={
                        "browser_attachment_error": str(exc),
                        "browser_attachment_cleanup_error": cleanup_detail,
                    },
                )
            raise

    def browser_detach(
        self,
        *,
        session_id: str,
        attachment_id: str,
    ) -> BrowserDetachmentResult:
        """Serialize browser capability revocation against gateway transitions."""
        with self._gateway_transition_lock(session_id):
            return self._browser_detach_serialized(
                session_id=session_id,
                attachment_id=attachment_id,
            )

    def _browser_detach_serialized(
        self,
        *,
        session_id: str,
        attachment_id: str,
    ) -> BrowserDetachmentResult:
        """Revoke one exact browser capability and stop its owned loopback proxy."""
        session = self.queue.get_gateway_session(session_id)
        if session.cluster != self.cluster:
            raise ConfigurationError(
                f"gateway session {session_id} belongs to cluster {session.cluster}, "
                f"not {self.cluster}"
            )
        session, result, cleanup = self._revoke_browser_attachment(
            session,
            attachment_id=attachment_id,
        )
        del session
        if cleanup.residual:
            raise RelayError(cleanup.detail or "browser attachment proxy cleanup failed")
        return result

    def _revoke_browser_attachment(
        self,
        session: GatewaySession,
        *,
        attachment_id: str,
    ) -> tuple[GatewaySession, BrowserDetachmentResult, CleanupResource]:
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
            result = BrowserDetachmentResult(
                attachment_id=record.attachment_id,
                revoked_at=cast(str, record.revoked_at),
                already_revoked=True,
                proxy_process_id=record.proxy_process_id,
                proxy_stopped=False,
            )
            return (
                session,
                result,
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
        stopped_pid, cleanup = self._stop_local_connector(
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
            result = BrowserDetachmentResult(
                attachment_id=attachment_id,
                revoked_at=revoked_at,
                already_revoked=False,
                proxy_process_id=record.proxy_process_id,
                proxy_stopped=False,
            )
            return session, result, cleanup
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
        persisted_revoked = BrowserAttachmentRecord.model_validate(
            session.gateway.get("browser_attachment")
        )
        effective_revoked_at = cast(str, persisted_revoked.revoked_at)
        return (
            session,
            BrowserDetachmentResult(
                attachment_id=attachment_id,
                revoked_at=effective_revoked_at,
                already_revoked=effective_revoked_at != revoked_at,
                proxy_process_id=record.proxy_process_id,
                proxy_stopped=stopped_pid is not None,
            ),
            cleanup,
        )

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
            session, _result, cleanup = self._revoke_browser_attachment(
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

"""Desktop-connector reattachment for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 16, class-mixin
split): the public ``attach`` entry point and its transition-lock-serialized
``_attach_serialized`` implementation. Attach either defers entirely to the
start-cluster resume path (a still-pending or pre-ready submission resumes
in place rather than treating this as a reattach), or recreates only the
desktop connector against an already-live remote connector, waiting for
local health before publishing readiness. A session that still holds a
JARVIS runtime binding is refused (``retired_jarvis_binding_message``).

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.queue``, ``self.settings``,
``self.definition``) and calls back into sibling mixins through ``self`` --
the detach mixin's resumability predicates and completed-detach consumption
(``self._detached_pending_submission_can_resume``,
``self._pre_ready_submission_can_resume``, ``self._completed_detach_result``,
``self._consume_completed_detach_for_attach``), the start mixin's resume
path and connector predicates (``self._resume_start_locked``,
``self._connector_reuse_is_verified``, ``self._connector_launch_is_authorized``,
``self._connector_recovery_pending``), reconciliation
(``self._reconcile_ownership_intents``), and connector lifecycle
(``self._local_connector_intent``, ``self._start_local_visitor``,
``self._stop_local_connector``, ``self._remove_unpublished_local_connector_files``).
Python's MRO resolves every one of those through whichever mixin defines it
regardless of call origin, so no cross-mixin qualification is used. The
class docstring in ``service_runtime.py`` records the full mixin
composition.
"""

from __future__ import annotations

from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_readiness as _readiness
from clio_relay import service_runtime_results as _results
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_service_runtime import retired_jarvis_binding_message
from clio_relay.models import GatewaySessionState, ServiceRuntimeSpec, utc_now


class _ServiceRuntimeAttachMixin:
    """Reattach the desktop connector to an already-live runtime."""

    def attach(
        self,
        *,
        session_id: str,
    ) -> _results.ServiceRuntimeStartResult | _results.ServiceRuntimePendingResult:
        """Serialize attachment against detach and teardown for this gateway."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        with self._gateway_transition_lock(session_id):
            return self._attach_serialized(session_id=session_id)

    def _attach_serialized(
        self,
        *,
        session_id: str,
    ) -> _results.ServiceRuntimeStartResult | _results.ServiceRuntimePendingResult:
        """Recreate the desktop connector while holding the gateway transition lock."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        if session.state == GatewaySessionState.CLOSED:
            raise ConfigurationError(f"gateway session {session_id} is closed")
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot attach"
            )
        if self._detached_pending_submission_can_resume(
            session
        ) or self._pre_ready_submission_can_resume(session):
            return self._resume_start_locked(session_id=session_id)
        if session.gateway.get("detach_intent") is not None:
            completed_detach = self._completed_detach_result(session)
            if completed_detach is None:
                raise ConfigurationError(
                    f"gateway session {session_id} has an incomplete detach; retry detach or "
                    "tear down the runtime"
                )
            session = self._consume_completed_detach_for_attach(session)
        session = self._reconcile_ownership_intents(session)
        spec = ServiceRuntimeSpec.model_validate(session.gateway["runtime_spec"])
        binding_document = session.gateway.get("jarvis_runtime_binding")
        if binding_document is not None:
            raise RelayError(retired_jarvis_binding_message(binding_document))
        transport = _primitives._object(session.gateway.get("transport", {}))
        remote_connector = _primitives._object(transport.get("remote_connector", {}))
        if not remote_connector or not self._connector_reuse_is_verified(
            session,
            role="remote_connector",
        ):
            return self._connector_recovery_pending(
                session,
                role="remote_connector",
            )
        proxy_name = _primitives._optional_str(transport.get("proxy_name"))
        if proxy_name is None:
            raise ConfigurationError("gateway session has no recorded transport proxy name")
        existing = _primitives._object(transport.get("desktop_connector", {}))
        existing_pid = _primitives._optional_int(existing.get("pid"))
        existing_config = _primitives._optional_str(existing.get("config_path"))
        existing_owned = (
            existing.get("owner") == "clio-relay"
            and existing.get("session_id") == session_id
            and existing_config is not None
        )
        created_connector = False
        local_connector: dict[str, object] | None = None
        try:
            if existing_pid is not None and existing_owned:
                if not self._connector_reuse_is_verified(
                    session,
                    role="desktop_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="desktop_connector",
                    )
                local_connector = existing
            else:
                if not self._connector_launch_is_authorized(
                    session,
                    role="desktop_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="desktop_connector",
                    )
                local_intent = self._local_connector_intent(session)
                session = self._set_ownership_intent(
                    session,
                    "desktop_connector",
                    local_intent,
                )
                local_connector = self._start_local_visitor(
                    session=session,
                    spec=spec,
                    proxy_name=proxy_name,
                    ownership_intent=local_intent,
                )
                created_connector = True
                session = self._update(
                    session,
                    gateway=self._gateway_with_ownership_intent(
                        session,
                        "desktop_connector",
                        _scheduler_contracts._new_ownership_intent("recorded", **local_connector),
                        transport={
                            **_primitives._object(session.gateway.get("transport", {})),
                            "desktop_connector": local_connector,
                        },
                    ),
                )
            connect_url = spec.connect_url_template.format(
                bind_addr=spec.desktop_bind_addr,
                bind_port=spec.desktop_bind_port,
                session_id=session.session_id,
            )
            health_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.health_path}"
            )
            try:
                self._wait_for_local_health(
                    health_url,
                    min(
                        spec.readiness_timeout_seconds,
                        _readiness._RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS,
                    ),
                    spec.poll_seconds,
                    expected_body=spec.health_expected_body,
                    max_attempts=1,
                )
            except RelayError as exc:
                pending = self._record_runtime_observation_pending(
                    session,
                    node=session.node,
                    error=exc,
                    provider_status=None,
                    state=GatewaySessionState.STARTING,
                    queue_state=session.queue_state or "running",
                    preserve_scheduler_status=True,
                )
                return _results.ServiceRuntimePendingResult(session=pending)
        except Exception as exc:
            cleanup_error: str | None = None
            if not created_connector:
                try:
                    recovered = self._reconcile_ownership_intents(
                        self.queue.get_gateway_session(session.session_id)
                    )
                    recovered_local = _primitives._object(
                        _primitives._object(recovered.gateway.get("transport", {})).get(
                            "desktop_connector",
                            {},
                        )
                    )
                    if recovered_local:
                        session = recovered
                        local_connector = recovered_local
                        created_connector = True
                except (ConfigurationError, RelayError) as recovery_exc:
                    cleanup_error = (
                        f"desktop connector rollback reconciliation failed: {recovery_exc}"
                    )
            if created_connector and local_connector is not None:
                _, rollback = self._stop_local_connector(
                    session_id=session.session_id,
                    connector=local_connector,
                    require_record=True,
                )
                if rollback.residual or not rollback.verified_after_operation:
                    cleanup_error = rollback.detail or "desktop connector rollback was not proven"
                else:
                    try:
                        self._remove_unpublished_local_connector_files(
                            session_id=session.session_id,
                            connector=local_connector,
                        )
                    except RelayError as cleanup_exc:
                        cleanup_error = str(cleanup_exc)
            self._record_attach_failure(
                session_id=session_id,
                error=exc,
                cleanup_error=cleanup_error,
            )
            raise
        try:
            stream_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.stream_path}"
                if spec.stream_path is not None
                else None
            )
            events_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.event_stream_path}"
                if spec.event_stream_path is not None
                else None
            )
            state_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.state_path}"
                if spec.state_path is not None
                else None
            )
            command_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.command_path}"
                if spec.command_path is not None
                else None
            )
            compatibility_urls = {
                name: (f"{spec.protocol}://{spec.desktop_bind_addr}:{spec.desktop_bind_port}{path}")
                for name, path in spec.compatibility_paths.items()
            }
            updated = self.queue.update_gateway_session(
                session_id,
                state=GatewaySessionState.READY,
                expected_updated_at=session.updated_at,
                metadata={"attached_at": utc_now().isoformat()},
                gateway={
                    **session.gateway,
                    "transport": {
                        **_primitives._object(session.gateway.get("transport", {})),
                        "desktop_connector": local_connector,
                    },
                },
            )
        except Exception as exc:
            cleanup_error: str | None = None
            if created_connector:
                _, rollback = self._stop_local_connector(
                    session_id=session.session_id,
                    connector=local_connector,
                    require_record=True,
                )
                if rollback.residual or not rollback.verified_after_operation:
                    cleanup_error = rollback.detail or "desktop connector rollback was not proven"
                else:
                    try:
                        self._remove_unpublished_local_connector_files(
                            session_id=session.session_id,
                            connector=local_connector,
                        )
                    except RelayError as cleanup_exc:
                        cleanup_error = str(cleanup_exc)
            self._record_attach_failure(
                session_id=session_id,
                error=exc,
                cleanup_error=cleanup_error,
            )
            raise
        return _results.ServiceRuntimeStartResult(
            session=updated,
            connect_url=connect_url,
            health_url=health_url,
            stream_url=stream_url,
            compatibility_urls=compatibility_urls,
            events_url=events_url,
            state_url=state_url,
            command_url=command_url,
        )

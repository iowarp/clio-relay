"""Runtime start/resume-start sequencing for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 11, class-mixin
split): the public ``start``/``resume_start`` entry points, the internal
``_resume_start_locked``/``_complete_runtime_start_locked`` state machine
that drives a submission from ``CREATED`` through ``READY``, the connector
readiness predicates that gate reuse vs. fresh-launch decisions
(``_first_unresolved_connector_role``, ``_connector_launch_is_authorized``,
``_connector_reuse_is_verified``, ``_connector_recovery_pending``,
``_scheduler_submission_reconciliation_is_pending``), the idempotent
``_ready_start_result`` rehydration, and the ``_rollback_runtime_start``
failure-path cleanup.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.queue``, ``self.settings``,
``self.definition``, ``self.cluster``) and calls back into methods owned by
sibling mixins through ``self`` -- most notably the JARVIS-bind resume path
(``self._validate_jarvis_binding_session``, ``self._resume_jarvis_binding_locked``),
the detach/resume-readiness predicates
(``self._detached_pending_submission_can_resume``, ``self._completed_detach_result``,
``self._consume_completed_detach_for_attach``), reconciliation
(``self._reconcile_ownership_intents``, ``self._verified_scheduler_submission``),
observation (``self._observe_allocation_and_health_once``,
``self._record_runtime_observation_pending``), connector lifecycle
(``self._local_connector_intent``, ``self._start_remote_connector``,
``self._start_local_visitor``, ``self._stop_local_connector``), and stop
(``self._stop_serialized``). Python's MRO resolves every one of those
through whichever mixin defines it regardless of call origin, so no
cross-mixin qualification is used. The class docstring in
``service_runtime.py`` records the full mixin composition.
"""

from __future__ import annotations

import secrets
from typing import cast

from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_readiness as _readiness
from clio_relay import service_runtime_results as _results
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_submission_scripts as _submission_scripts
from clio_relay import service_runtime_types as _types
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_remote_scripts import remote_stop_script as _remote_stop_script
from clio_relay.jarvis_service_runtime import reverify_jarvis_service_runtime
from clio_relay.models import GatewaySession, GatewaySessionState, ServiceRuntimeSpec, utc_now
from clio_relay.scheduler_providers import provider_for_scheduler


class _ServiceRuntimeStartMixin:
    """Start a scheduler-backed remote service and advance it to readiness."""

    def start(
        self,
        *,
        name: str,
        spec: ServiceRuntimeSpec,
        owner_session_id: str | None = None,
        owner_session_generation_id: str | None = None,
        owner_session_admission_id: str | None = None,
    ) -> _results.ServiceRuntimeStartResult | _results.ServiceRuntimePendingResult:
        """Start a scheduler-backed remote service and bind it to a desktop port."""
        if spec.deployment_driver == "jarvis-bound":
            raise ConfigurationError("jarvis-bound runtimes must use bind_verified_jarvis_runtime")
        if spec.submit_command is None:
            raise ConfigurationError("submitted runtimes require a submit command")
        submit_command = spec.submit_command
        if (owner_session_id is None) != (owner_session_generation_id is None):
            raise ConfigurationError(
                "owner_session_id and owner_session_generation_id must be provided together"
            )
        if owner_session_admission_id is not None and owner_session_id is None:
            raise ConfigurationError(
                "owner_session_admission_id requires owner_session_id and generation"
            )
        scheduler_provider = provider_for_scheduler(spec.scheduler)
        if scheduler_provider.name != spec.scheduler:
            spec = spec.model_copy(update={"scheduler": scheduler_provider.name})
        self.queue.initialize()
        owner_metadata: dict[str, object] = {
            "owner": "clio-relay",
            "runtime_kind": spec.kind,
        }
        if owner_session_id is not None and owner_session_generation_id is not None:
            owner_metadata.update(
                {
                    "owner_session_id": owner_session_id,
                    "owner_session_generation_id": owner_session_generation_id,
                }
            )
            if owner_session_admission_id is not None:
                owner_metadata["owner_session_admission_id"] = owner_session_admission_id
        session = self.queue.create_gateway_session(
            GatewaySession(
                cluster=self.cluster,
                name=name,
                state=GatewaySessionState.CREATED,
                scheduler=spec.scheduler,
                requested_resources={"service_port": spec.service_port},
                gateway={
                    "runtime_spec": spec.model_dump(mode="json"),
                    "transport": {"mode": spec.transport_mode},
                    "ownership_intents": {
                        role: _scheduler_contracts._new_ownership_intent("not_started")
                        for role in (
                            "scheduler_submission",
                            "remote_connector",
                            "desktop_connector",
                        )
                    },
                },
                metadata=owner_metadata,
            )
        )
        transition_lock = self._acquire_gateway_transition_lock(session.session_id)
        try:
            session = self._runtime_start_session_after_lock(session.session_id)
        except BaseException:
            transition_lock.release()
            raise
        completion_started = False
        try:
            session = self._update(
                session,
                state=GatewaySessionState.SUBMITTED,
                metadata={"submitted_at": utc_now().isoformat()},
            )
            submission_id = secrets.token_hex(16)
            submission_marker = secrets.token_hex(32)
            session = self._set_ownership_intent(
                session,
                "scheduler_submission",
                _scheduler_contracts._new_ownership_intent(
                    "starting",
                    submission_id=submission_id,
                    scheduler_provider=spec.scheduler,
                    submission_marker=submission_marker,
                ),
            )
            try:
                submit_output = self._ssh(
                    _submission_scripts._submit_script(
                        submit_command,
                        session_id=session.session_id,
                        submission_id=submission_id,
                        scheduler_provider=spec.scheduler,
                        submission_marker=submission_marker,
                    )
                )
            except _types._AmbiguousRemoteSideEffectError as exc:
                pending = self._record_runtime_observation_pending(
                    self.queue.get_gateway_session(session.session_id),
                    node=None,
                    error=exc,
                    provider_status=None,
                    state=GatewaySessionState.PENDING,
                )
                return _results.ServiceRuntimePendingResult(session=pending)
            submission = _scheduler_contracts._parse_runtime_submission(submit_output)
            scheduler_job_id = submission.scheduler_job_id
            session = self._update(
                session,
                scheduler_job_id=scheduler_job_id,
                queue_state="submitted",
                gateway=self._gateway_with_ownership_intent(
                    session,
                    "scheduler_submission",
                    _scheduler_contracts._new_ownership_intent(
                        "recorded",
                        submission_id=submission_id,
                        scheduler_provider=spec.scheduler,
                        submission_marker=submission_marker,
                        scheduler_job_id=scheduler_job_id,
                    ),
                    submit_output=submit_output.strip(),
                ),
            )
            node = self._observe_allocation_and_health_once(
                session,
                spec,
                scheduler_job_id,
                initial_service_host=submission.service_host,
            )
            if node is None:
                return _results.ServiceRuntimePendingResult(
                    session=self.queue.get_gateway_session(session.session_id)
                )
            completion_started = True
            return self._complete_runtime_start_locked(
                session_id=session.session_id,
                spec=spec,
                node=node,
            )
        except Exception as exc:
            if not completion_started:
                self._rollback_runtime_start(
                    session_id=session.session_id,
                    error=exc,
                    remote_connector=None,
                    local_connector=None,
                )
            raise
        finally:
            transition_lock.release()

    def resume_start(
        self,
        *,
        session_id: str,
    ) -> _results.ServiceRuntimeStartResult | _results.ServiceRuntimePendingResult:
        """Advance one exact durable runtime submission without resubmitting it."""
        self.queue.initialize()
        with self._gateway_transition_lock(session_id):
            return self._resume_start_locked(session_id=session_id)

    def _resume_start_locked(
        self,
        *,
        session_id: str,
    ) -> _results.ServiceRuntimeStartResult | _results.ServiceRuntimePendingResult:
        """Advance one durable start while the caller holds its transition lock."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        binding_document = session.gateway.get("jarvis_runtime_binding")
        if binding_document is not None:
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
            spec = self._validate_jarvis_binding_session(
                session=session,
                verified=verified_runtime,
            )
            if session.gateway.get("teardown_intent") is not None:
                raise ConfigurationError(
                    f"gateway session {session_id} is committed to teardown and cannot resume"
                )
            if session.state is GatewaySessionState.READY:
                return self._ready_start_result(session)
            authorization = self._jarvis_runtime_authorization(verified_runtime)
            return self._resume_jarvis_binding_locked(
                session_id=session_id,
                verified=verified_runtime,
                authorization=authorization,
                readiness_timeout_seconds=spec.readiness_timeout_seconds,
                poll_seconds=spec.poll_seconds,
            )
        if session.state is GatewaySessionState.READY:
            return self._ready_start_result(session)
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot resume"
            )
        session = self._reconcile_ownership_intents(session)
        if session.state is GatewaySessionState.FAILED:
            scheduler_intent = _primitives._object(
                _primitives._object(session.gateway.get("ownership_intents", {})).get(
                    "scheduler_submission",
                    {},
                )
            )
            if scheduler_intent.get("reconciliation_outcome") == "definitive_failure":
                raise RelayError(
                    _primitives._optional_str(scheduler_intent.get("reconciliation_error"))
                    or "scheduler submission reconciliation failed definitively"
                )
        if session.state is GatewaySessionState.DEGRADED:
            if not self._detached_pending_submission_can_resume(session):
                raise ConfigurationError(
                    f"gateway session {session_id} cannot resume start from {session.state.value}"
                )
            completed_detach = self._completed_detach_result(session)
            if completed_detach is None:
                raise ConfigurationError(
                    f"gateway session {session_id} has an incomplete detach; retry detach or "
                    "tear down the runtime"
                )
            session = self._consume_completed_detach_for_attach(session)
            session = self._update(
                session,
                state=(
                    GatewaySessionState.ALLOCATED
                    if session.node is not None
                    else GatewaySessionState.PENDING
                ),
                metadata={
                    "cleanup_retryable": None,
                    "cleanup_errors": [],
                },
            )
        if session.state not in {
            GatewaySessionState.SUBMITTED,
            GatewaySessionState.PENDING,
            GatewaySessionState.ALLOCATED,
            GatewaySessionState.STARTING,
        }:
            raise ConfigurationError(
                f"gateway session {session_id} cannot resume start from {session.state.value}"
            )
        unresolved_connector = self._first_unresolved_connector_role(session)
        if unresolved_connector is not None:
            return self._connector_recovery_pending(
                session,
                role=unresolved_connector,
            )
        try:
            submission = self._verified_scheduler_submission(session)
        except RelayError as exc:
            if (
                session.scheduler_job_id is None
                and not self._scheduler_submission_reconciliation_is_pending(session)
            ):
                raise
            pending_session = self._record_runtime_observation_pending(
                session,
                node=session.node,
                error=exc,
                provider_status=None,
            )
            return _results.ServiceRuntimePendingResult(session=pending_session)
        try:
            node = self._observe_allocation_and_health_once(
                session,
                submission.spec,
                submission.scheduler_job_id,
                initial_service_host=session.node,
            )
        except _types._DefinitiveRuntimeObservationError as exc:
            self._rollback_runtime_start(
                session_id=session_id,
                error=exc,
                remote_connector=None,
                local_connector=None,
            )
            raise
        if node is None:
            return _results.ServiceRuntimePendingResult(
                session=self.queue.get_gateway_session(session_id)
            )
        return self._complete_runtime_start_locked(
            session_id=session_id,
            spec=submission.spec,
            node=node,
        )

    def _complete_runtime_start_locked(
        self,
        *,
        session_id: str,
        spec: ServiceRuntimeSpec,
        node: str,
    ) -> _results.ServiceRuntimeStartResult | _results.ServiceRuntimePendingResult:
        """Create connectors and publish readiness while holding the session transition lock."""
        session = self._reconcile_ownership_intents(self.queue.get_gateway_session(session_id))
        remote_connector: dict[str, object] | None = None
        local_connector: dict[str, object] | None = None
        try:
            proxy_name = spec.proxy_name or f"{session.session_id}-service"
            session = self._update(
                session,
                state=GatewaySessionState.STARTING,
                queue_state="running",
                node=node,
            )
            transport = _primitives._object(session.gateway.get("transport", {}))
            recovered_remote = _primitives._object(transport.get("remote_connector", {}))
            if recovered_remote:
                if not self._connector_reuse_is_verified(
                    session,
                    role="remote_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="remote_connector",
                    )
                remote_connector = recovered_remote
            else:
                if not self._connector_launch_is_authorized(
                    session,
                    role="remote_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="remote_connector",
                    )
                remote_intent = _scheduler_contracts._new_ownership_intent(
                    "starting",
                    owner_token=secrets.token_hex(32),
                    connector_generation_id=secrets.token_hex(16),
                )
                session = self._set_ownership_intent(
                    session,
                    "remote_connector",
                    remote_intent,
                )
                try:
                    remote_connector = self._start_remote_connector(
                        session=session,
                        spec=spec,
                        node=node,
                        proxy_name=proxy_name,
                        ownership_intent=remote_intent,
                    )
                except _types._AmbiguousRemoteSideEffectError as exc:
                    latest = self.queue.get_gateway_session(session.session_id)
                    pending = self._record_runtime_observation_pending(
                        latest,
                        node=node,
                        error=exc,
                        provider_status=None,
                        state=GatewaySessionState.STARTING,
                        queue_state=latest.queue_state or "running",
                        preserve_scheduler_status=True,
                    )
                    return _results.ServiceRuntimePendingResult(session=pending)
                session = self.queue.get_gateway_session(session.session_id)
                session = self._update(
                    session,
                    gateway=self._gateway_with_ownership_intent(
                        session,
                        "remote_connector",
                        _scheduler_contracts._new_ownership_intent("recorded", **remote_connector),
                        transport={
                            **_primitives._object(session.gateway.get("transport", {})),
                            "proxy_name": proxy_name,
                            "remote_connector": remote_connector,
                        },
                    ),
                )
            transport = _primitives._object(session.gateway.get("transport", {}))
            recovered_local = _primitives._object(transport.get("desktop_connector", {}))
            if recovered_local:
                if not self._connector_reuse_is_verified(
                    session,
                    role="desktop_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="desktop_connector",
                    )
                local_connector = recovered_local
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
                session = self._update(
                    session,
                    gateway=self._gateway_with_ownership_intent(
                        session,
                        "desktop_connector",
                        _scheduler_contracts._new_ownership_intent("recorded", **local_connector),
                        transport={
                            **_primitives._object(session.gateway.get("transport", {})),
                            "proxy_name": proxy_name,
                            "remote_connector": remote_connector,
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
                pending_session = self._record_runtime_observation_pending(
                    session,
                    node=node,
                    error=exc,
                    provider_status=None,
                    state=GatewaySessionState.STARTING,
                    queue_state=session.queue_state or "running",
                    preserve_scheduler_status=True,
                )
                return _results.ServiceRuntimePendingResult(session=pending_session)
            events_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.event_stream_path}"
                if spec.event_stream_path is not None
                else None
            )
            stream_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.stream_path}"
                if spec.stream_path is not None
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
            session = self._update(
                session,
                state=GatewaySessionState.READY,
                queue_state="running",
                node=node,
                gateway={
                    **session.gateway,
                    "connect_url": connect_url,
                    "health_url": health_url,
                    "stream_url": stream_url,
                    "compatibility_urls": compatibility_urls,
                    "events_url": events_url,
                    "state_url": state_url,
                    "command_url": command_url,
                    "service": {
                        "host": node,
                        "port": spec.service_port,
                        "health_path": spec.health_path,
                        "stream_mode": spec.stream_mode,
                        "stream_path": spec.stream_path,
                        "compatibility_paths": spec.compatibility_paths,
                        "state_path": spec.state_path,
                        "event_stream_path": spec.event_stream_path,
                        "command_path": spec.command_path,
                        "protocol": spec.protocol,
                        "deployment_driver": spec.deployment_driver,
                    },
                    "transport": {
                        "mode": spec.transport_mode,
                        "proxy_name": proxy_name,
                        "remote_connector": remote_connector,
                        "desktop_connector": local_connector,
                        "remote_target": f"{node}:{spec.service_port}",
                        "desktop_bind": f"{spec.desktop_bind_addr}:{spec.desktop_bind_port}",
                    },
                },
                metadata={"ready_at": utc_now().isoformat()},
            )
            return _results.ServiceRuntimeStartResult(
                session=session,
                connect_url=connect_url,
                health_url=health_url,
                stream_url=stream_url,
                compatibility_urls=compatibility_urls,
                events_url=events_url,
                state_url=state_url,
                command_url=command_url,
            )
        except Exception as exc:
            self._rollback_runtime_start(
                session_id=session_id,
                error=exc,
                remote_connector=remote_connector,
                local_connector=local_connector,
            )
            raise

    def _ready_start_result(self, session: GatewaySession) -> _results.ServiceRuntimeStartResult:
        """Rehydrate an idempotent ready result from one exact durable gateway record."""
        gateway = session.gateway
        connect_url = _primitives._optional_str(gateway.get("connect_url"))
        health_url = _primitives._optional_str(gateway.get("health_url"))
        if connect_url is None or health_url is None:
            raise RelayError("ready gateway session omitted its durable connection URLs")
        compatibility_raw = gateway.get("compatibility_urls")
        compatibility_urls = (
            {
                key: value
                for key, value in cast(dict[object, object], compatibility_raw).items()
                if isinstance(key, str) and isinstance(value, str)
            }
            if isinstance(compatibility_raw, dict)
            else {}
        )
        return _results.ServiceRuntimeStartResult(
            session=session,
            connect_url=connect_url,
            health_url=health_url,
            stream_url=_primitives._optional_str(gateway.get("stream_url")),
            compatibility_urls=compatibility_urls,
            events_url=_primitives._optional_str(gateway.get("events_url")),
            state_url=_primitives._optional_str(gateway.get("state_url")),
            command_url=_primitives._optional_str(gateway.get("command_url")),
        )

    @staticmethod
    def _first_unresolved_connector_role(session: GatewaySession) -> str | None:
        """Return one connector whose exact durable identity remains ambiguous."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        transport = _primitives._object(session.gateway.get("transport", {}))
        for role in ("remote_connector", "desktop_connector"):
            intent = _primitives._object(intents.get(role, {}))
            record = _primitives._object(transport.get(role, {}))
            if intent.get("reconciliation_error") is not None:
                return role
            if intent.get("state") in {"starting", "recorded"} and (
                not record or intent.get("live_identity_verified") is not True
            ):
                return role
            if record and intent.get("state") != "recorded":
                return role
        return None

    @staticmethod
    def _scheduler_submission_reconciliation_is_pending(session: GatewaySession) -> bool:
        """Return whether one exact pre-submit identity still awaits sidecar publication."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get("scheduler_submission", {}))
        return bool(
            session.scheduler_job_id is None
            and intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and intent.get("state") == "starting"
            and _primitives._optional_str(intent.get("submission_id")) is not None
            and _primitives._optional_str(intent.get("submission_marker")) is not None
            and intent.get("scheduler_provider") == session.scheduler
        )

    @staticmethod
    def _connector_launch_is_authorized(
        session: GatewaySession,
        *,
        role: str,
    ) -> bool:
        """Allow a new generation only after durable non-start or exact absence proof."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get(role, {}))
        transport = _primitives._object(session.gateway.get("transport", {}))
        return bool(
            intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and intent.get("state") in {"not_started", "absent_verified"}
            and not _primitives._object(transport.get(role, {}))
            and intent.get("reconciliation_error") is None
        )

    @staticmethod
    def _connector_reuse_is_verified(
        session: GatewaySession,
        *,
        role: str,
    ) -> bool:
        """Require fresh live reconciliation before adopting a durable connector record."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get(role, {}))
        transport = _primitives._object(session.gateway.get("transport", {}))
        return bool(
            _primitives._object(transport.get(role, {}))
            and intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and intent.get("state") == "recorded"
            and intent.get("live_identity_verified") is True
            and intent.get("reconciliation_error") is None
        )

    def _connector_recovery_pending(
        self,
        session: GatewaySession,
        *,
        role: str,
    ) -> _results.ServiceRuntimePendingResult:
        """Persist an ambiguous connector identity as resumable, without replacement."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get(role, {}))
        detail = _primitives._optional_str(intent.get("reconciliation_error"))
        error = RelayError(
            detail or f"{role.replace('_', ' ')} identity has not been proven live for this intent"
        )
        pending = self._record_runtime_observation_pending(
            session,
            node=session.node,
            error=error,
            provider_status=None,
            state=GatewaySessionState.STARTING,
            queue_state=session.queue_state or "running",
            preserve_scheduler_status=True,
        )
        return _results.ServiceRuntimePendingResult(session=pending)

    def _rollback_runtime_start(
        self,
        *,
        session_id: str,
        error: BaseException,
        remote_connector: dict[str, object] | None,
        local_connector: dict[str, object] | None,
    ) -> None:
        """Roll back owned connectors while retaining the submitted scheduler job."""
        cleanup_errors: list[str] = []
        if remote_connector is None:
            try:
                recovered = self._reconcile_ownership_intents(
                    self.queue.get_gateway_session(session_id)
                )
                recovered_remote = _primitives._object(
                    _primitives._object(recovered.gateway.get("transport", {})).get(
                        "remote_connector",
                        {},
                    )
                )
                if recovered_remote:
                    remote_connector = recovered_remote
            except (ConfigurationError, RelayError) as recovery_exc:
                cleanup_errors.append(
                    f"remote connector rollback reconciliation failed: {recovery_exc}"
                )
        if local_connector is not None:
            _, local_rollback = self._stop_local_connector(
                session_id=session_id,
                connector=local_connector,
                require_record=True,
            )
            if local_rollback.residual or not local_rollback.verified_after_operation:
                cleanup_errors.append(
                    local_rollback.detail or "desktop connector rollback was not proven"
                )
        if remote_connector is not None:
            remote_pid = _primitives._optional_int(remote_connector.get("pid"))
            if remote_pid is None:
                cleanup_errors.append("remote connector rollback has no recorded pid")
            else:
                try:
                    remote_result = _scheduler_contracts._last_json_object(
                        self._ssh(
                            _remote_stop_script(
                                session_id=session_id,
                                pid=remote_pid,
                            )
                        )
                    )
                    if not _connector_identity._remote_cleanup_proven(remote_result):
                        cleanup_errors.append(
                            "remote connector rollback did not prove full process-group absence"
                        )
                except RelayError as rollback_exc:
                    cleanup_errors.append(str(rollback_exc))
        try:
            stop_result = self._stop_serialized(
                session_id=session_id,
                cancel_scheduler_job=False,
                final_state=GatewaySessionState.FAILED,
            )
            cleanup_errors.extend(stop_result.errors)
        except Exception as cleanup_exc:
            cleanup_errors.append(str(cleanup_exc))
        try:
            self._record_runtime_start_failure(
                session_id=session_id,
                error=error,
                cleanup_errors=cleanup_errors,
            )
        except Exception as record_exc:
            error.add_note(
                f"runtime failure handling could not persist its final record: {record_exc}"
            )

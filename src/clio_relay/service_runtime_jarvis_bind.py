"""JARVIS-bound runtime binding for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 12, class-mixin
split): the deterministic bind entry point ``bind_verified_jarvis_runtime``
(binding + owner identity derive the gateway ID, so a replay resumes rather
than duplicates), its identity/policy helpers
(``_jarvis_bind_owner_identity``, ``_jarvis_bind_identity``,
``_jarvis_bind_policy``, ``_jarvis_runtime_spec``), the fail-closed replay
guard ``_validate_jarvis_binding_session``, the ``_resume_jarvis_binding_locked``
state machine that advances a bound runtime to readiness,
``_jarvis_connector_start_intent``, and the definitive-failure cleanup
``_rollback_jarvis_binding``. The two schema constants
(``_JARVIS_BIND_IDENTITY_SCHEMA``, ``_JARVIS_BIND_POLICY_SCHEMA``) move with
their only callers.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.queue``, ``self.cluster``) and
calls back into sibling mixins through ``self`` -- the connector
readiness/recovery predicates and ``_ready_start_result``
(``_ServiceRuntimeStartMixin``), the detach resume path
(``_attach_serialized``), reconciliation (``self._reconcile_ownership_intents``),
observation (``self._record_runtime_observation_pending``,
``self._poll_scheduler_provider``), and remote/local connector lifecycle
(``self._start_remote_connector``, ``self._start_local_visitor``,
``self._stop_local_connector``, ``self._stop_allocation_connector``).
Python's MRO resolves every one of those through whichever mixin defines it
regardless of call origin, so no cross-mixin qualification is used. The
class docstring in ``service_runtime.py`` records the full mixin
composition.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import subprocess
from typing import Literal

from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_readiness as _readiness
from clio_relay import service_runtime_results as _results
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_types as _types
from clio_relay.errors import ConfigurationError, NotFoundError, RelayError
from clio_relay.frp_remote_scripts import remote_stop_script as _remote_stop_script
from clio_relay.jarvis_service_runtime import (
    JarvisServiceRuntimeBinding,
    VerifiedJarvisServiceRuntime,
)
from clio_relay.models import (
    GatewaySession,
    GatewaySessionState,
    SchedulerPhase,
    SchedulerStatus,
    ServiceRuntimeSpec,
    utc_now,
)
from clio_relay.owner_session_admission import desktop_owner_session_admission_id

_JARVIS_BIND_IDENTITY_SCHEMA = "clio-relay.jarvis-bind-identity.v1"
_JARVIS_BIND_POLICY_SCHEMA = "clio-relay.jarvis-bind-policy.v1"


class _ServiceRuntimeJarvisBindMixin:
    """Bind, resume, and roll back a JARVIS-owned service runtime."""

    def bind_verified_jarvis_runtime(
        self,
        *,
        name: str,
        verified: VerifiedJarvisServiceRuntime,
        desktop_bind_port: int | None = None,
        owner_session_id: str | None = None,
        owner_session_generation_id: str | None = None,
        owner_session_admission_id: str | None = None,
        transport_mode: str = "frp-stcp-wss",
        readiness_timeout_seconds: float = 300.0,
        poll_seconds: float = 2.0,
    ) -> _results.ServiceRuntimeStartResult | _results.ServiceRuntimePendingResult:
        """Bind or resume one exact JARVIS-owned service without submitting work.

        The immutable binding and owner identity derive the gateway ID. Reissuing
        an identical request therefore resumes the same connector intents; it
        cannot create a second gateway, scheduler job, or untracked connector.
        """
        runtime = verified.runtime
        binding = verified.binding
        if runtime.lifecycle != "ready":
            raise ConfigurationError("only a ready JARVIS service runtime can be bound")
        if (owner_session_id is None) != (owner_session_generation_id is None):
            raise ConfigurationError(
                "owner_session_id and owner_session_generation_id must be provided together"
            )
        if owner_session_admission_id is not None and owner_session_id is None:
            raise ConfigurationError(
                "owner_session_admission_id requires owner_session_id and generation"
            )
        if owner_session_id is not None and owner_session_admission_id is None:
            raise ConfigurationError(
                "owned JARVIS runtime binding requires owner_session_admission_id"
            )
        if owner_session_id is not None and owner_session_admission_id != (
            desktop_owner_session_admission_id(
                cluster=self.cluster,
                session_id=owner_session_id,
            )
        ):
            raise ConfigurationError(
                "owned JARVIS runtime binding admission id does not match its "
                "cluster/session identity"
            )
        if readiness_timeout_seconds <= 0 or poll_seconds <= 0:
            raise ConfigurationError("runtime readiness intervals must be positive")
        if binding.scheduler_native_id is not None:
            if binding.scheduler_provider is None:
                raise ConfigurationError(
                    "scheduler-backed JARVIS runtime omitted its scheduler provider"
                )
            if runtime.host not in {"127.0.0.1", "::1", "localhost"}:
                raise ConfigurationError(
                    "scheduler-backed JARVIS services must advertise a loopback-only endpoint"
                )

        owner_identity = self._jarvis_bind_owner_identity(
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            owner_session_admission_id=owner_session_admission_id,
        )
        session_id, identity_sha256 = self._jarvis_bind_identity(
            binding=binding,
            owner_identity=owner_identity,
        )
        requested_policy = self._jarvis_bind_policy(
            name=name,
            transport_mode=transport_mode,
            requested_desktop_bind_port=desktop_bind_port,
        )
        self.queue.initialize()
        # The deterministic transition lock is also the binding-creation lock.
        # No process can race a lookup/create pair for this immutable identity.
        with self._gateway_transition_lock(session_id):
            try:
                session = self.queue.get_gateway_session(session_id)
            except NotFoundError:
                # Resolve authenticated authority before creating any durable or
                # process side effect. Authorization/integrity failures fail closed.
                authorization = self._jarvis_runtime_authorization(verified)
                local_port = (
                    _readiness._available_loopback_port(exclude={runtime.port})
                    if desktop_bind_port is None
                    else _readiness._validated_available_loopback_port(desktop_bind_port)
                )
                spec = self._jarvis_runtime_spec(
                    verified=verified,
                    local_port=local_port,
                    transport_mode=transport_mode,
                    readiness_timeout_seconds=readiness_timeout_seconds,
                    poll_seconds=poll_seconds,
                )
                policy = {
                    **requested_policy,
                    "actual_desktop_bind_port": local_port,
                }
                owner_metadata: dict[str, object] = {
                    "owner": "clio-relay",
                    "runtime_kind": spec.kind,
                    "binding_source": "jarvis_mcp_result",
                    "jarvis_bind_identity_sha256": identity_sha256,
                    "source_relay_job_id": binding.source_relay_job_id,
                    "source_relay_artifact_id": binding.source_relay_artifact_id,
                    "jarvis_execution_id": binding.jarvis_execution_id,
                    **{key: value for key, value in owner_identity.items() if value is not None},
                }
                session = self.queue.create_gateway_session(
                    GatewaySession(
                        session_id=session_id,
                        cluster=self.cluster,
                        name=name,
                        state=GatewaySessionState.CREATED,
                        scheduler=binding.scheduler_provider or "external",
                        scheduler_job_id=binding.scheduler_native_id,
                        requested_resources={"service_port": runtime.port},
                        gateway={
                            "runtime_spec": spec.model_dump(mode="json"),
                            "jarvis_runtime_binding": binding.model_dump(mode="json"),
                            "jarvis_bind_policy": policy,
                            "transport": {"mode": transport_mode},
                            "ownership_intents": {
                                "scheduler_submission": _scheduler_contracts._new_ownership_intent(
                                    "absent_verified",
                                    source="verified_jarvis_runtime_binding",
                                ),
                                "remote_connector": _scheduler_contracts._new_ownership_intent(
                                    "not_started"
                                ),
                                "desktop_connector": _scheduler_contracts._new_ownership_intent(
                                    "not_started"
                                ),
                            },
                        },
                        metadata=owner_metadata,
                    )
                )
                session = self._runtime_start_session_after_lock(session.session_id)
            else:
                self._validate_jarvis_binding_session(
                    session=session,
                    verified=verified,
                    expected_policy=requested_policy,
                    expected_owner_identity=owner_identity,
                )
                if session.gateway.get("teardown_intent") is not None:
                    raise ConfigurationError(
                        f"gateway session {session.session_id} is committed to teardown "
                        "and cannot resume"
                    )
                if session.state is GatewaySessionState.READY:
                    return self._ready_start_result(session)
                authorization = self._jarvis_runtime_authorization(verified)
            return self._resume_jarvis_binding_locked(
                session_id=session.session_id,
                verified=verified,
                authorization=authorization,
                readiness_timeout_seconds=readiness_timeout_seconds,
                poll_seconds=poll_seconds,
            )

    @staticmethod
    def _jarvis_bind_owner_identity(
        *,
        owner_session_id: str | None,
        owner_session_generation_id: str | None,
        owner_session_admission_id: str | None,
    ) -> dict[str, object]:
        """Return the complete owner identity used by deterministic JARVIS binds."""
        return {
            "owner_session_id": owner_session_id,
            "owner_session_generation_id": owner_session_generation_id,
            "owner_session_admission_id": owner_session_admission_id,
        }

    def _jarvis_bind_identity(
        self,
        *,
        binding: JarvisServiceRuntimeBinding,
        owner_identity: dict[str, object],
    ) -> tuple[str, str]:
        """Derive one portable gateway ID from immutable binding and owner identity."""
        document = {
            "schema_version": _JARVIS_BIND_IDENTITY_SCHEMA,
            "cluster": self.cluster,
            "binding": binding.model_dump(mode="json"),
            "owner_identity": owner_identity,
        }
        encoded = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        return f"gateway_{digest[:32]}", digest

    @staticmethod
    def _jarvis_bind_policy(
        *,
        name: str,
        transport_mode: str,
        requested_desktop_bind_port: int | None,
    ) -> dict[str, object]:
        """Return side-effect policy that cannot change on an idempotent replay."""
        return {
            "schema_version": _JARVIS_BIND_POLICY_SCHEMA,
            "name": name,
            "transport_mode": transport_mode,
            "requested_desktop_bind_port": requested_desktop_bind_port,
        }

    @staticmethod
    def _jarvis_runtime_spec(
        *,
        verified: VerifiedJarvisServiceRuntime,
        local_port: int,
        transport_mode: str,
        readiness_timeout_seconds: float,
        poll_seconds: float,
    ) -> ServiceRuntimeSpec:
        """Build the generic connector spec from verified JARVIS endpoints only."""
        runtime = verified.runtime
        return ServiceRuntimeSpec(
            kind="jarvis-service-runtime",
            submit_command=None,
            deployment_driver="jarvis-bound",
            service_port=runtime.port,
            protocol=runtime.protocol,
            health_path=runtime.health_path,
            stream_mode="push",
            stream_path=runtime.live_data_path,
            event_stream_path=runtime.events_path,
            state_path=runtime.state_path,
            command_path=runtime.command_path,
            desktop_bind_addr="127.0.0.1",
            desktop_bind_port=local_port,
            transport_mode=transport_mode,
            readiness_timeout_seconds=readiness_timeout_seconds,
            poll_seconds=poll_seconds,
            scheduler=verified.binding.scheduler_provider or "external",
            connect_url_template=f"{runtime.protocol}://{{bind_addr}}:{{bind_port}}",
            metadata={
                "source": "verified_jarvis_service_runtime",
                "service_instance_id": runtime.service_instance_id,
                "service_revision": runtime.revision,
            },
        )

    def _validate_jarvis_binding_session(
        self,
        *,
        session: GatewaySession,
        verified: VerifiedJarvisServiceRuntime,
        expected_policy: dict[str, object] | None = None,
        expected_owner_identity: dict[str, object] | None = None,
    ) -> ServiceRuntimeSpec:
        """Fail closed if a persisted JARVIS binding or immutable policy changed."""
        self._validate_gateway_transition_session(session)
        try:
            stored_binding = JarvisServiceRuntimeBinding.model_validate(
                session.gateway.get("jarvis_runtime_binding")
            )
            spec = ServiceRuntimeSpec.model_validate(session.gateway.get("runtime_spec"))
        except ValueError as exc:
            raise RelayError("JARVIS gateway binding evidence is invalid") from exc
        if stored_binding != verified.binding:
            raise ConfigurationError("JARVIS gateway binding identity changed")
        owner_identity = self._jarvis_bind_owner_identity(
            owner_session_id=_primitives._optional_str(session.metadata.get("owner_session_id")),
            owner_session_generation_id=_primitives._optional_str(
                session.metadata.get("owner_session_generation_id")
            ),
            owner_session_admission_id=_primitives._optional_str(
                session.metadata.get("owner_session_admission_id")
            ),
        )
        expected_session_id, identity_sha256 = self._jarvis_bind_identity(
            binding=stored_binding,
            owner_identity=owner_identity,
        )
        if (
            session.session_id != expected_session_id
            or session.metadata.get("jarvis_bind_identity_sha256") != identity_sha256
        ):
            raise RelayError("JARVIS gateway deterministic binding identity is invalid")
        if expected_owner_identity is not None and owner_identity != expected_owner_identity:
            raise ConfigurationError("JARVIS gateway owner identity changed")
        policy = _primitives._object(session.gateway.get("jarvis_bind_policy", {}))
        actual_port = policy.get("actual_desktop_bind_port")
        if (
            policy.get("schema_version") != _JARVIS_BIND_POLICY_SCHEMA
            or policy.get("name") != session.name
            or policy.get("transport_mode") != spec.transport_mode
            or isinstance(actual_port, bool)
            or not isinstance(actual_port, int)
            or actual_port != spec.desktop_bind_port
        ):
            raise RelayError("JARVIS gateway immutable bind policy is invalid")
        if expected_policy is not None and any(
            policy.get(key) != value for key, value in expected_policy.items()
        ):
            raise ConfigurationError(
                "JARVIS runtime is already bound with a different immutable policy"
            )
        runtime = verified.runtime
        expected_scheduler = stored_binding.scheduler_provider or "external"
        if (
            spec.deployment_driver != "jarvis-bound"
            or spec.kind != "jarvis-service-runtime"
            or session.scheduler != expected_scheduler
            or session.scheduler_job_id != stored_binding.scheduler_native_id
            or spec.scheduler != expected_scheduler
            or spec.service_port != runtime.port
            or spec.protocol != runtime.protocol
            or spec.health_path != runtime.health_path
            or spec.stream_path != runtime.live_data_path
            or spec.event_stream_path != runtime.events_path
            or spec.state_path != runtime.state_path
            or spec.command_path != runtime.command_path
            or spec.desktop_bind_addr != "127.0.0.1"
            or spec.transport_mode != policy.get("transport_mode")
        ):
            raise RelayError("JARVIS gateway endpoints or scheduler identity changed")
        return spec

    def _resume_jarvis_binding_locked(
        self,
        *,
        session_id: str,
        verified: VerifiedJarvisServiceRuntime,
        authorization: str | None,
        readiness_timeout_seconds: float,
        poll_seconds: float,
    ) -> _results.ServiceRuntimeStartResult | _results.ServiceRuntimePendingResult:
        """Advance one exact JARVIS binding while holding its transition lock."""
        session = self.queue.get_gateway_session(session_id)
        spec = self._validate_jarvis_binding_session(session=session, verified=verified)
        if session.state is GatewaySessionState.READY:
            return self._ready_start_result(session)
        if session.state in {GatewaySessionState.FAILED, GatewaySessionState.CLOSED}:
            raise ConfigurationError(
                f"gateway session {session_id} cannot resume from {session.state.value}"
            )
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot resume"
            )
        if session.state is GatewaySessionState.DEGRADED:
            return self._attach_serialized(session_id=session_id)
        if session.state not in {
            GatewaySessionState.CREATED,
            GatewaySessionState.SUBMITTED,
            GatewaySessionState.PENDING,
            GatewaySessionState.ALLOCATED,
            GatewaySessionState.STARTING,
        }:
            raise ConfigurationError(
                f"gateway session {session_id} cannot resume from {session.state.value}"
            )
        runtime = verified.runtime
        if runtime.lifecycle != "ready":
            raise ConfigurationError("JARVIS service runtime is no longer ready")
        if session.state is GatewaySessionState.CREATED:
            session = self._update(
                session,
                state=GatewaySessionState.STARTING,
                queue_state=runtime.lifecycle,
                node=runtime.host,
                metadata={"binding_started_at": utc_now().isoformat()},
            )
        session = self._reconcile_ownership_intents(session)
        unresolved_connector = self._first_unresolved_connector_role(session)
        if unresolved_connector is not None:
            return self._connector_recovery_pending(session, role=unresolved_connector)

        proxy_name = f"{session.session_id}-service"
        transport = _primitives._object(session.gateway.get("transport", {}))
        remote_connector = _primitives._object(transport.get("remote_connector", {}))
        if remote_connector:
            if not self._connector_reuse_is_verified(session, role="remote_connector"):
                return self._connector_recovery_pending(session, role="remote_connector")
        else:
            if not self._connector_launch_is_authorized(session, role="remote_connector"):
                return self._connector_recovery_pending(session, role="remote_connector")
            remote_intent = self._jarvis_connector_start_intent(
                session,
                role="remote_connector",
            )
            session = self._set_ownership_intent(session, "remote_connector", remote_intent)
            try:
                remote_connector = self._start_remote_connector(
                    session=session,
                    spec=spec,
                    node=runtime.host,
                    proxy_name=proxy_name,
                    ownership_intent=remote_intent,
                    allocation_provider=verified.binding.scheduler_provider,
                    allocation_job_id=verified.binding.scheduler_native_id,
                )
            except _types._AmbiguousRemoteSideEffectError as exc:
                latest = self.queue.get_gateway_session(session.session_id)
                pending = self._record_runtime_observation_pending(
                    latest,
                    node=runtime.host,
                    error=exc,
                    provider_status=None,
                    state=GatewaySessionState.STARTING,
                    queue_state=runtime.lifecycle,
                    preserve_scheduler_status=True,
                )
                return _results.ServiceRuntimePendingResult(session=pending)
            except RelayError as exc:
                provider_status: SchedulerStatus | None = None
                if (
                    verified.binding.scheduler_provider is not None
                    and verified.binding.scheduler_native_id is not None
                ):
                    try:
                        provider_status = self._poll_scheduler_provider(
                            provider=verified.binding.scheduler_provider,
                            scheduler_job_id=verified.binding.scheduler_native_id,
                        )
                    except RelayError:
                        provider_status = None
                if provider_status is not None and provider_status.phase in {
                    SchedulerPhase.COMPLETED,
                    SchedulerPhase.FAILED,
                    SchedulerPhase.CANCELED,
                }:
                    definitive = _types._DefinitiveRuntimeObservationError(
                        "scheduler job reached a terminal state before its verified JARVIS "
                        "service could be bound: "
                        f"job={verified.binding.scheduler_native_id} "
                        f"state={provider_status.phase.value}"
                    )
                    self._rollback_jarvis_binding(session_id=session_id, error=definitive)
                    raise definitive from exc
                latest = self.queue.get_gateway_session(session.session_id)
                pending = self._record_runtime_observation_pending(
                    latest,
                    node=runtime.host,
                    error=exc,
                    provider_status=provider_status,
                    state=GatewaySessionState.STARTING,
                    queue_state=(
                        provider_status.phase.value
                        if provider_status is not None
                        else runtime.lifecycle
                    ),
                    preserve_scheduler_status=provider_status is None,
                )
                return _results.ServiceRuntimePendingResult(session=pending)
            # Allocation connector startup can publish placement intent first.
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
        local_connector = _primitives._object(transport.get("desktop_connector", {}))
        if local_connector:
            if not self._connector_reuse_is_verified(session, role="desktop_connector"):
                return self._connector_recovery_pending(session, role="desktop_connector")
        else:
            if not self._connector_launch_is_authorized(session, role="desktop_connector"):
                return self._connector_recovery_pending(session, role="desktop_connector")
            local_intent = self._jarvis_connector_start_intent(
                session,
                role="desktop_connector",
            )
            session = self._set_ownership_intent(session, "desktop_connector", local_intent)
            try:
                local_connector = self._start_local_visitor(
                    session=session,
                    spec=spec,
                    proxy_name=proxy_name,
                    ownership_intent=local_intent,
                )
            except (RelayError, OSError, subprocess.SubprocessError) as exc:
                pending = self._record_runtime_observation_pending(
                    self.queue.get_gateway_session(session.session_id),
                    node=runtime.host,
                    error=RelayError(str(exc)),
                    provider_status=None,
                    state=GatewaySessionState.STARTING,
                    queue_state=runtime.lifecycle,
                    preserve_scheduler_status=True,
                )
                return _results.ServiceRuntimePendingResult(session=pending)
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

        local_port = spec.desktop_bind_port
        base_url = f"{runtime.protocol}://127.0.0.1:{local_port}"
        connect_url = base_url
        health_url = f"{base_url}{runtime.health_path}"
        stream_url = f"{base_url}{runtime.live_data_path}"
        events_url = f"{base_url}{runtime.events_path}"
        state_url = f"{base_url}{runtime.state_path}"
        command_url = f"{base_url}{runtime.command_path}"
        try:
            self._wait_for_jarvis_health(
                health_url,
                timeout_seconds=min(
                    readiness_timeout_seconds,
                    _readiness._RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS,
                ),
                poll_seconds=poll_seconds,
                runtime_schema_version=runtime.schema_version,
                authorization=authorization,
                max_attempts=1,
            )
        except _types._DefinitiveRuntimeObservationError as exc:
            self._rollback_jarvis_binding(session_id=session_id, error=exc)
            raise
        except RelayError as exc:
            pending = self._record_runtime_observation_pending(
                session,
                node=runtime.host,
                error=exc,
                provider_status=None,
                state=GatewaySessionState.STARTING,
                queue_state=runtime.lifecycle,
                preserve_scheduler_status=True,
            )
            return _results.ServiceRuntimePendingResult(session=pending)

        session = self._update(
            session,
            state=GatewaySessionState.READY,
            queue_state=runtime.lifecycle,
            node=runtime.host,
            gateway={
                **session.gateway,
                "connect_url": connect_url,
                "health_url": health_url,
                "stream_url": stream_url,
                "events_url": events_url,
                "state_url": state_url,
                "command_url": command_url,
                "compatibility_urls": {},
                "service": {
                    "host": runtime.host,
                    "port": runtime.port,
                    "protocol": runtime.protocol,
                    "health_path": runtime.health_path,
                    "stream_mode": runtime.delivery_mode,
                    "stream_path": runtime.live_data_path,
                    "event_stream_path": runtime.events_path,
                    "state_path": runtime.state_path,
                    "command_path": runtime.command_path,
                    "deployment_driver": "jarvis-bound",
                    "placement": remote_connector.get("placement"),
                },
                "transport": {
                    "mode": spec.transport_mode,
                    "proxy_name": proxy_name,
                    "remote_connector": remote_connector,
                    "desktop_connector": local_connector,
                    "remote_target": f"{runtime.host}:{runtime.port}",
                    "desktop_bind": f"127.0.0.1:{local_port}",
                },
            },
            metadata={"ready_at": utc_now().isoformat()},
        )
        return _results.ServiceRuntimeStartResult(
            session=session,
            connect_url=connect_url,
            health_url=health_url,
            stream_url=stream_url,
            compatibility_urls={},
            events_url=events_url,
            state_url=state_url,
            command_url=command_url,
        )

    def _jarvis_connector_start_intent(
        self,
        session: GatewaySession,
        *,
        role: Literal["remote_connector", "desktop_connector"],
    ) -> dict[str, object]:
        """Reuse an absence-proven generation instead of inventing a retry identity."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        previous = _primitives._object(intents.get(role, {}))
        if previous.get("state") != "absent_verified":
            if role == "desktop_connector":
                return self._local_connector_intent(session)
            return _scheduler_contracts._new_ownership_intent(
                "starting",
                owner_token=secrets.token_hex(32),
                connector_generation_id=secrets.token_hex(16),
            )
        identity: dict[str, object] = {
            "owner_token": _scheduler_contracts._required_intent_str(previous, "owner_token"),
            "connector_generation_id": _scheduler_contracts._required_intent_str(
                previous,
                "connector_generation_id",
            ),
        }
        if role == "desktop_connector":
            for field in (
                "config_path",
                "stdout_path",
                "stderr_path",
                "metadata_path",
            ):
                identity[field] = _scheduler_contracts._required_intent_str(previous, field)
        return _scheduler_contracts._new_ownership_intent("starting", **identity)

    def _rollback_jarvis_binding(
        self,
        *,
        session_id: str,
        error: BaseException,
    ) -> None:
        """Fail closed and clean exact connectors after a definitive bind failure."""
        cleanup_errors: list[str] = []
        local_connector: dict[str, object] | None = None
        remote_connector: dict[str, object] | None = None
        try:
            recovered = self._reconcile_ownership_intents(
                self.queue.get_gateway_session(session_id)
            )
            transport = _primitives._object(recovered.gateway.get("transport", {}))
            local_connector = _primitives._object(transport.get("desktop_connector", {})) or None
            remote_connector = _primitives._object(transport.get("remote_connector", {})) or None
        except (ConfigurationError, RelayError) as exc:
            cleanup_errors.append(f"connector rollback reconciliation failed: {exc}")
        if local_connector is not None:
            try:
                _, result = self._stop_local_connector(
                    session_id=session_id,
                    connector=local_connector,
                    require_record=True,
                )
                if result.residual or not result.verified_after_operation:
                    cleanup_errors.append(
                        result.detail or "desktop connector rollback was not proven"
                    )
            except (ConfigurationError, RelayError) as exc:
                cleanup_errors.append(str(exc))
        if remote_connector is not None:
            try:
                if remote_connector.get("execution_scope") == "scheduler_allocation":
                    result = self._stop_allocation_connector(
                        session_id=session_id,
                        connector=remote_connector,
                    )
                    if result.residual or not result.verified_after_operation:
                        cleanup_errors.append(
                            result.detail or "allocation connector rollback was not proven"
                        )
                else:
                    remote_pid = _primitives._optional_int(remote_connector.get("pid"))
                    if remote_pid is None:
                        raise RelayError("remote connector rollback has no recorded pid")
                    result = _scheduler_contracts._last_json_object(
                        self._ssh(
                            _remote_stop_script(
                                session_id=session_id,
                                pid=remote_pid,
                            )
                        )
                    )
                    if not _connector_identity._remote_cleanup_proven(result):
                        cleanup_errors.append(
                            "remote connector rollback did not prove process-group absence"
                        )
            except (ConfigurationError, RelayError) as exc:
                cleanup_errors.append(str(exc))
        self._record_runtime_start_failure(
            session_id=session_id,
            error=error,
            cleanup_errors=cleanup_errors,
        )

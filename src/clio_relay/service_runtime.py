"""Generic supervisor for scheduler-backed streaming service sessions."""

from __future__ import annotations

import json
import math
import os
import secrets
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, cast

import httpx

from clio_relay import service_runtime_browser as _browser
from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_connector_step_scripts as _connector_step_scripts
from clio_relay import service_runtime_core as _core
from clio_relay import service_runtime_detach as _detach
from clio_relay import service_runtime_jarvis_bind as _jarvis_bind
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_readiness as _readiness
from clio_relay import service_runtime_results as _results
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_start as _start
from clio_relay import service_runtime_stop as _stop
from clio_relay import service_runtime_submission_scripts as _submission_scripts
from clio_relay import service_runtime_types as _types
from clio_relay.browser_gateway import (
    CAPABILITY_ENV,
    UPSTREAM_AUTHORIZATION_ENV,
    BrowserGatewayBootstrap,
    BrowserGatewayConfig,
)
from clio_relay.errors import (
    ConfigurationError,
    QueueConflictError,
    RelayError,
)
from clio_relay.frp_link import FrpLinkConfig, render_proxy_config, start_owned_frp_visitor
from clio_relay.frp_remote_scripts import (
    remote_allocation_frpc_start_script as _remote_allocation_frpc_start_script,
)
from clio_relay.frp_remote_scripts import (
    remote_frpc_start_script as _remote_frpc_start_script,
)
from clio_relay.jarvis_service_runtime import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V1,
    JARVIS_SERVICE_RUNTIME_SCHEMA_V2,
    VerifiedJarvisServiceRuntime,
    reverify_jarvis_service_runtime,
)
from clio_relay.models import (
    GatewaySession,
    GatewaySessionState,
    SchedulerConnectorPlacement,
    SchedulerConnectorStepIdentity,
    SchedulerConnectorStepStatus,
    SchedulerPhase,
    SchedulerStatus,
    ServiceRuntimeSpec,
    utc_now,
)
from clio_relay.owner_session_admission import desktop_owner_session_admission_id
from clio_relay.relay_host import FrpTransportProtocol
from clio_relay.scheduler_providers import (
    SchedulerAllocationConnectorProvider,
    provider_for_scheduler,
)
from clio_relay.service_runtime_results import (
    ServiceRuntimePendingResult,  # noqa: F401 -- cli.py/mcp_server.py/live_acceptance.py bare-import this
    ServiceRuntimeStartResult,  # noqa: F401 -- cli.py bare-imports this; see rationale above
    ServiceRuntimeStopResult,  # noqa: F401 -- cli.py/mcp_server.py/live_acceptance.py bare-import this
)
from clio_relay.session_wire_models import CleanupResource

_LOCAL_CONNECTOR_WRAPPER_CODE = (
    "import subprocess,sys; "
    "_owner_token=sys.argv[1]; "
    "_generation_id=sys.argv[2]; "
    "child=subprocess.Popen(sys.argv[3:]); "
    "raise SystemExit(child.wait())"
)
_MAX_LOCAL_HEALTH_BYTES = 64 * 1024
_CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS = 30.0
_CONNECTOR_STEP_CLEANUP_POLL_SECONDS = 0.25


class ServiceRuntimeSupervisor(
    _core._ServiceRuntimeCoreMixin,
    _start._ServiceRuntimeStartMixin,
    _jarvis_bind._ServiceRuntimeJarvisBindMixin,
    _browser._ServiceRuntimeBrowserMixin,
    _stop._ServiceRuntimeStopMixin,
    _detach._ServiceRuntimeDetachMixin,
):
    """Start, bind, probe, and tear down scheduler-backed remote service sessions.

    Composed from owner-module mixins (#231 class-mixin split, §9): each
    mixin owns one coherent slice of the state machine's methods; this class
    is assembly only. See each mixin's own module docstring for its exact
    method set. Mixins call each other freely through ``self`` -- Python's
    MRO resolves ``self.other_method(...)`` to whichever mixin defines it
    regardless of where the call originates, so no cross-mixin qualification
    is needed or used.
    """

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
        verified_runtime: VerifiedJarvisServiceRuntime | None = None
        service_authorization: str | None = None
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
            runtime = verified_runtime.runtime
            if runtime.lifecycle != "ready":
                raise ConfigurationError("detached JARVIS service runtime is no longer ready")
            if (
                spec.deployment_driver != "jarvis-bound"
                or runtime.port != spec.service_port
                or runtime.protocol != spec.protocol
                or runtime.health_path != spec.health_path
                or runtime.live_data_path != spec.stream_path
                or runtime.events_path != spec.event_stream_path
                or runtime.state_path != spec.state_path
                or runtime.command_path != spec.command_path
            ):
                raise RelayError("detached JARVIS runtime endpoints changed before reattachment")
            service_authorization = self._jarvis_runtime_authorization(verified_runtime)
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
                if verified_runtime is None:
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
                else:
                    self._wait_for_jarvis_health(
                        health_url,
                        timeout_seconds=min(
                            spec.readiness_timeout_seconds,
                            _readiness._RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS,
                        ),
                        poll_seconds=spec.poll_seconds,
                        runtime_schema_version=verified_runtime.runtime.schema_version,
                        authorization=service_authorization,
                        max_attempts=1,
                    )
            except _types._DefinitiveRuntimeObservationError as exc:
                self._rollback_jarvis_binding(session_id=session_id, error=exc)
                raise
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

    def _local_connector_intent(self, session: GatewaySession) -> dict[str, object]:
        """Build the exact durable identity needed to rediscover a local connector."""
        runtime_dir = (
            self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        ).resolve()
        return _scheduler_contracts._new_ownership_intent(
            "starting",
            owner_token=secrets.token_hex(32),
            connector_generation_id=secrets.token_hex(16),
            config_path=str(runtime_dir / "desktop-frpc.toml"),
            stdout_path=str(runtime_dir / "desktop-frpc.out"),
            stderr_path=str(runtime_dir / "desktop-frpc.err"),
            metadata_path=str(runtime_dir / "desktop-frpc-owner.json"),
        )

    def _validate_remote_connector_intent_binding(
        self,
        *,
        session_id: str,
        intent: dict[str, object],
        connector: dict[str, object],
    ) -> None:
        """Require a complete remote connector identity bound to one durable intent."""
        if (
            intent.get("schema_version") != _primitives._OWNERSHIP_INTENT_SCHEMA
            or intent.get("state") not in {"starting", "recorded"}
            or connector.get("owner") != "clio-relay"
            or connector.get("session_id") != session_id
            or connector.get("owner_token")
            != _scheduler_contracts._required_intent_str(intent, "owner_token")
            or connector.get("connector_generation_id")
            != _scheduler_contracts._required_intent_str(intent, "connector_generation_id")
        ):
            raise RelayError("remote connector record does not match its durable intent")
        common_fields = (
            "owner",
            "session_id",
            "owner_token",
            "connector_generation_id",
        )
        if intent.get("state") == "recorded" and any(
            intent.get(field) != connector.get(field) for field in common_fields
        ):
            raise RelayError("recorded remote connector identity changed after publication")
        if connector.get("execution_scope") == "scheduler_allocation":
            self._allocation_connector_identity(
                session_id=session_id,
                connector=connector,
            )
            allocation_fields = (
                "execution_scope",
                "scheduler_provider",
                "scheduler_native_id",
                "scheduler_step_id",
                "scheduler_step_marker",
                "scheduler_step",
                "placement",
                "config_path",
                "log_path",
            )
            if intent.get("state") == "recorded" and any(
                intent.get(field) != connector.get(field) for field in allocation_fields
            ):
                raise RelayError("recorded allocation connector identity changed after publication")
            return
        pid = _primitives._optional_int(connector.get("pid"))
        process_group_id = _primitives._optional_int(connector.get("process_group_id"))
        config_path = _primitives._optional_str(connector.get("config_path"))
        log_path = _primitives._optional_str(connector.get("log_path"))
        if pid is None or process_group_id != pid or config_path is None or log_path is None:
            raise RelayError("remote connector record has incomplete process identity")
        validated_config = _scheduler_contracts._validated_remote_session_file(
            config_path,
            session_id=session_id,
            filename="remote-frpc.toml",
        )
        validated_log = _scheduler_contracts._validated_remote_session_file(
            log_path,
            session_id=session_id,
            filename="remote-frpc.log",
        )
        if validated_config.parent != validated_log.parent:
            raise RelayError("remote connector paths do not belong to one owned session")
        process_fields = (
            "pid",
            "process_group_id",
            "config_path",
            "log_path",
        )
        if intent.get("state") == "recorded" and any(
            intent.get(field) != connector.get(field) for field in process_fields
        ):
            raise RelayError("recorded remote connector process identity changed after publication")

    def _validate_local_connector_intent_binding(
        self,
        *,
        session_id: str,
        intent: dict[str, object],
        connector: dict[str, object],
    ) -> None:
        """Require a complete desktop connector identity bound to one durable intent."""
        if (
            intent.get("schema_version") != _primitives._OWNERSHIP_INTENT_SCHEMA
            or intent.get("state") not in {"starting", "recorded"}
            or connector.get("owner") != "clio-relay"
            or connector.get("session_id") != session_id
            or connector.get("owner_token")
            != _scheduler_contracts._required_intent_str(intent, "owner_token")
            or connector.get("connector_generation_id")
            != _scheduler_contracts._required_intent_str(intent, "connector_generation_id")
        ):
            raise RelayError("desktop connector record does not match its durable intent")
        pid = _primitives._optional_int(connector.get("pid"))
        process_group_id = _primitives._optional_int(connector.get("process_group_id"))
        start_marker = _primitives._optional_str(connector.get("process_start_marker"))
        if pid is None or process_group_id is None or start_marker is None:
            raise RelayError("desktop connector record has incomplete process identity")
        runtime_dir = (self.settings.core_dir.parent / "runtime-sessions" / session_id).resolve()
        path_fields = (
            "config_path",
            "stdout_path",
            "stderr_path",
            "metadata_path",
        )
        for field in path_fields:
            value = _primitives._optional_str(connector.get(field))
            if value is None or Path(value).resolve().parent != runtime_dir:
                raise RelayError("desktop connector record escaped its owned runtime directory")
        identity_fields = (
            "owner",
            "session_id",
            "pid",
            "process_group_id",
            "process_start_marker",
            "owner_token",
            "connector_generation_id",
            *path_fields,
        )
        if intent.get("state") == "recorded" and any(
            intent.get(field) != connector.get(field) for field in identity_fields
        ):
            raise RelayError("recorded desktop connector identity changed after publication")

    @staticmethod
    def _connector_records_match(
        first: dict[str, object],
        second: dict[str, object],
        *,
        fields: Sequence[str],
    ) -> bool:
        """Return whether two records name the same complete connector generation."""
        return all(first.get(field) == second.get(field) for field in fields)

    def _reconcile_ownership_intents(self, session: GatewaySession) -> GatewaySession:
        """Recover scheduler and connector identities written before a hard exit."""
        gateway = dict(session.gateway)
        intents = _primitives._object(gateway.get("ownership_intents", {}))
        if not intents:
            return session
        transport = _primitives._object(gateway.get("transport", {}))
        changed = False
        scheduler_job_id = session.scheduler_job_id
        definitive_submission_failure: _types._DefinitiveSubmissionReconciliationError | None = None

        scheduler_intent = _primitives._object(intents.get("scheduler_submission", {}))
        if scheduler_job_id is None and scheduler_intent.get("state") == "recorded":
            recorded_scheduler_job_id = _primitives._optional_str(
                scheduler_intent.get("scheduler_job_id")
            )
            if recorded_scheduler_job_id is not None:
                scheduler_job_id = recorded_scheduler_job_id
                changed = True
        if (
            scheduler_job_id is None
            and scheduler_intent.get("state") == "starting"
            and scheduler_intent.get("reconciliation_outcome") != "definitive_failure"
        ):
            submission_id = _primitives._optional_str(scheduler_intent.get("submission_id"))
            scheduler_provider = _primitives._optional_str(
                scheduler_intent.get("scheduler_provider")
            )
            submission_marker = _primitives._optional_str(scheduler_intent.get("submission_marker"))
            if (
                submission_id is not None
                and scheduler_provider is not None
                and submission_marker is not None
            ):
                try:
                    record = _scheduler_contracts._last_json_object(
                        self._ssh(
                            _submission_scripts._remote_submission_record_script(
                                session_id=session.session_id,
                                submission_id=submission_id,
                                scheduler_provider=scheduler_provider,
                                submission_marker=submission_marker,
                            )
                        )
                    )
                    if (
                        record.get("schema_version")
                        == _submission_scripts._REMOTE_SUBMISSION_VERIFICATION_SCHEMA
                        and record.get("verification_outcome") == "definitive_invalid"
                    ):
                        reported_error = _primitives._optional_str(record.get("error"))
                        message = (
                            reported_error[:1024]
                            if reported_error is not None
                            else "scheduler submission sidecar failed integrity verification"
                        )
                        failure_kind: Literal["integrity_failure"] = "integrity_failure"
                        raise _types._DefinitiveSubmissionReconciliationError(
                            message,
                            evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                session_id=session.session_id,
                                submission_id=submission_id,
                                scheduler_provider=scheduler_provider,
                                submission_marker=submission_marker,
                                record=record,
                                error=message,
                                failure_kind=failure_kind,
                            ),
                            failure_kind=failure_kind,
                        )
                    if record.get("present") is True:
                        output = record.get("output")
                        if (
                            record.get("session_id") != session.session_id
                            or record.get("submission_id") != submission_id
                            or record.get("scheduler_provider") != scheduler_provider
                            or record.get("submission_marker") != submission_marker
                        ):
                            message = "scheduler submission sidecar identity is invalid"
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="integrity_failure",
                                ),
                                failure_kind="integrity_failure",
                            )
                        returncode = record.get("returncode")
                        if isinstance(returncode, bool) or not isinstance(returncode, int):
                            message = "scheduler submission sidecar return code is invalid"
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="integrity_failure",
                                ),
                                failure_kind="integrity_failure",
                            )
                        if returncode != 0:
                            message = (
                                "scheduler submission command completed unsuccessfully: "
                                f"returncode={returncode}"
                            )
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="command_failure",
                                ),
                                failure_kind="command_failure",
                            )
                        if not isinstance(output, str):
                            message = "scheduler submission sidecar output is invalid"
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="integrity_failure",
                                ),
                                failure_kind="integrity_failure",
                            )
                        try:
                            submission = _scheduler_contracts._parse_runtime_submission(output)
                        except RelayError as exc:
                            message = f"scheduler submission sidecar output is invalid: {exc}"
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="response_invalid",
                                ),
                                failure_kind="response_invalid",
                            ) from exc
                        scheduler_job_id = submission.scheduler_job_id
                        intents["scheduler_submission"] = (
                            _scheduler_contracts._new_ownership_intent(
                                "recorded",
                                submission_id=submission_id,
                                scheduler_provider=scheduler_provider,
                                submission_marker=submission_marker,
                                scheduler_job_id=scheduler_job_id,
                                reconciled=True,
                            )
                        )
                        gateway["submit_output"] = output.strip()
                        changed = True
                except _types._DefinitiveSubmissionReconciliationError as exc:
                    failed_intent = dict(scheduler_intent)
                    failed_intent["reconciliation_error"] = str(exc)
                    failed_intent["reconciliation_outcome"] = "definitive_failure"
                    failed_intent["reconciliation_failure_kind"] = exc.failure_kind
                    failed_intent["failure_evidence"] = exc.evidence
                    intents["scheduler_submission"] = failed_intent
                    definitive_submission_failure = exc
                    changed = True
                except RelayError as exc:
                    unresolved_intent = dict(scheduler_intent)
                    unresolved_intent["reconciliation_error"] = str(exc)
                    unresolved_intent["reconciliation_outcome"] = "observation_unknown"
                    intents["scheduler_submission"] = unresolved_intent
                    changed = True

        remote_intent = _primitives._object(intents.get("remote_connector", {}))
        remote_record = _primitives._object(transport.get("remote_connector", {}))
        if remote_intent.get("state") in {"starting", "recorded"}:
            try:
                if remote_record:
                    self._validate_remote_connector_intent_binding(
                        session_id=session.session_id,
                        intent=remote_intent,
                        connector=remote_record,
                    )
                owner_token = _scheduler_contracts._required_intent_str(
                    remote_intent, "owner_token"
                )
                generation_id = _scheduler_contracts._required_intent_str(
                    remote_intent,
                    "connector_generation_id",
                )
                allocation_placement = _primitives._object(remote_intent.get("placement", {}))
                result = _scheduler_contracts._last_json_object(
                    self._ssh(
                        _connector_step_scripts._remote_connector_discovery_script(
                            session_id=session.session_id,
                            owner_token=owner_token,
                            connector_generation_id=generation_id,
                            allocation_provider=_primitives._optional_str(
                                remote_intent.get("scheduler_provider")
                            ),
                            allocation_job_id=_primitives._optional_str(
                                remote_intent.get("scheduler_native_id")
                            ),
                            allocation_step_marker=_primitives._optional_str(
                                remote_intent.get("scheduler_step_marker")
                            ),
                            allocation_placement_host=_primitives._optional_str(
                                allocation_placement.get("placement_host")
                            ),
                        )
                    )
                )
                connector = result.get("connector")
                verified_connector: dict[str, object] | None = None
                absence_verified = False
                if remote_intent.get("execution_scope") == "scheduler_allocation":
                    if result.get("ownership_verified") is not True:
                        detail = result.get("error")
                        raise RelayError(
                            detail
                            if isinstance(detail, str)
                            else "allocation connector sidecar could not be verified"
                        )
                    verified_connector, absence_verified = (
                        self._reconcile_allocation_connector_intent(
                            session_id=session.session_id,
                            intent=remote_intent,
                            connector_base=(
                                cast(dict[str, object], connector)
                                if isinstance(connector, dict)
                                else None
                            ),
                        )
                    )
                elif (
                    result.get("ownership_verified") is True
                    and result.get("present") is True
                    and isinstance(connector, dict)
                ):
                    verified_connector = cast(dict[str, object], connector)
                elif (
                    result.get("ownership_verified") is True
                    and result.get("present") is False
                    and result.get("matching_pids") == []
                ):
                    absence_verified = True
                else:
                    detail = result.get("error")
                    raise RelayError(
                        detail
                        if isinstance(detail, str)
                        else "remote connector ownership observation was incomplete"
                    )
                if verified_connector is not None:
                    self._validate_remote_connector_intent_binding(
                        session_id=session.session_id,
                        intent=remote_intent,
                        connector=verified_connector,
                    )
                    remote_fields = (
                        "owner",
                        "session_id",
                        "pid",
                        "process_group_id",
                        "execution_scope",
                        "scheduler_provider",
                        "scheduler_native_id",
                        "scheduler_step_id",
                        "scheduler_step_marker",
                        "scheduler_step",
                        "connector_generation_id",
                        "owner_token",
                        "config_path",
                        "log_path",
                        "placement",
                    )
                    if remote_record and not self._connector_records_match(
                        remote_record,
                        verified_connector,
                        fields=remote_fields,
                    ):
                        raise RelayError(
                            "remote connector record disagrees with its live sidecar identity"
                        )
                    transport["remote_connector"] = verified_connector
                    intents["remote_connector"] = _scheduler_contracts._new_ownership_intent(
                        "recorded",
                        reconciled=True,
                        live_identity_verified=True,
                        **verified_connector,
                    )
                    changed = True
                elif absence_verified:
                    transport.pop("remote_connector", None)
                    intents["remote_connector"] = _scheduler_contracts._new_ownership_intent(
                        "absent_verified",
                        owner_token=owner_token,
                        connector_generation_id=generation_id,
                        execution_scope=remote_intent.get("execution_scope"),
                        scheduler_provider=remote_intent.get("scheduler_provider"),
                        scheduler_native_id=remote_intent.get("scheduler_native_id"),
                        scheduler_step_marker=remote_intent.get("scheduler_step_marker"),
                        placement=remote_intent.get("placement"),
                        reconciled=True,
                    )
                    changed = True
            except RelayError as exc:
                unresolved_remote = dict(remote_intent)
                unresolved_remote.pop("live_identity_verified", None)
                unresolved_remote["reconciliation_error"] = str(exc)
                intents["remote_connector"] = unresolved_remote
                changed = True
        elif remote_record:
            unresolved_remote = dict(remote_intent)
            unresolved_remote["reconciliation_error"] = (
                "remote connector record has no matching starting or recorded durable intent"
            )
            intents["remote_connector"] = unresolved_remote
            changed = True

        local_intent = _primitives._object(intents.get("desktop_connector", {}))
        local_record = _primitives._object(transport.get("desktop_connector", {}))
        if local_intent.get("state") in {"starting", "recorded"}:
            try:
                if local_record:
                    self._validate_local_connector_intent_binding(
                        session_id=session.session_id,
                        intent=local_intent,
                        connector=local_record,
                    )
                connector, absence_verified = _connector_identity._discover_local_connector(
                    local_intent,
                    session_id=session.session_id,
                )
                if connector is not None:
                    self._validate_local_connector_intent_binding(
                        session_id=session.session_id,
                        intent=local_intent,
                        connector=connector,
                    )
                    local_fields = (
                        "owner",
                        "session_id",
                        "pid",
                        "process_group_id",
                        "process_start_marker",
                        "owner_token",
                        "connector_generation_id",
                        "config_path",
                        "stdout_path",
                        "stderr_path",
                        "metadata_path",
                    )
                    if local_record and not self._connector_records_match(
                        local_record,
                        connector,
                        fields=local_fields,
                    ):
                        raise RelayError(
                            "desktop connector record disagrees with its live sidecar identity"
                        )
                    transport["desktop_connector"] = connector
                    intents["desktop_connector"] = _scheduler_contracts._new_ownership_intent(
                        "recorded",
                        reconciled=True,
                        live_identity_verified=True,
                        **connector,
                    )
                    changed = True
                elif absence_verified:
                    transport.pop("desktop_connector", None)
                    intents["desktop_connector"] = _scheduler_contracts._new_ownership_intent(
                        "absent_verified",
                        owner_token=local_intent.get("owner_token"),
                        connector_generation_id=local_intent.get("connector_generation_id"),
                        config_path=local_intent.get("config_path"),
                        stdout_path=local_intent.get("stdout_path"),
                        stderr_path=local_intent.get("stderr_path"),
                        metadata_path=local_intent.get("metadata_path"),
                        reconciled=True,
                    )
                    changed = True
            except RelayError as exc:
                unresolved_local = dict(local_intent)
                unresolved_local.pop("live_identity_verified", None)
                unresolved_local["reconciliation_error"] = str(exc)
                intents["desktop_connector"] = unresolved_local
                changed = True
        elif local_record:
            unresolved_local = dict(local_intent)
            unresolved_local["reconciliation_error"] = (
                "desktop connector record has no matching starting or recorded durable intent"
            )
            intents["desktop_connector"] = unresolved_local
            changed = True

        if not changed:
            return session
        gateway["ownership_intents"] = intents
        gateway["transport"] = transport
        if definitive_submission_failure is not None:
            return self._update(
                session,
                state=GatewaySessionState.FAILED,
                queue_state=definitive_submission_failure.queue_state,
                gateway=gateway,
                metadata={
                    "failed_at": utc_now().isoformat(),
                    "last_error": str(definitive_submission_failure),
                    "runtime_observation_error": str(definitive_submission_failure),
                    "scheduler_submission_outcome": (
                        definitive_submission_failure.scheduler_submission_outcome
                    ),
                },
            )
        if scheduler_job_id is not None:
            return self._update(
                session,
                gateway=gateway,
                scheduler_job_id=scheduler_job_id,
                queue_state=session.queue_state or "submitted",
            )
        return self._update(session, gateway=gateway)

    def _reconcile_allocation_connector_intent(
        self,
        *,
        session_id: str,
        intent: dict[str, object],
        connector_base: dict[str, object] | None,
    ) -> tuple[dict[str, object] | None, bool]:
        """Recover or disprove an allocation connector by its provider marker."""
        provider_name = _scheduler_contracts._required_intent_str(intent, "scheduler_provider")
        scheduler_job_id = _scheduler_contracts._required_intent_str(intent, "scheduler_native_id")
        step_marker = _scheduler_contracts._required_intent_str(intent, "scheduler_step_marker")
        generation_id = _scheduler_contracts._required_intent_str(intent, "connector_generation_id")
        try:
            placement = SchedulerConnectorPlacement.model_validate_json(
                json.dumps(intent.get("placement"), separators=(",", ":"), allow_nan=False)
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("allocation connector intent has invalid placement") from exc
        if (
            intent.get("execution_scope") != "scheduler_allocation"
            or placement.scheduler != provider_name
            or placement.scheduler_job_id != scheduler_job_id
            or step_marker != _scheduler_contracts._connector_step_marker(session_id, generation_id)
        ):
            raise RelayError("allocation connector recovery identity does not match its intent")
        if connector_base is not None and (
            connector_base.get("owner") != "clio-relay"
            or connector_base.get("session_id") != session_id
            or connector_base.get("execution_scope") != "scheduler_allocation"
            or connector_base.get("scheduler_provider") != provider_name
            or connector_base.get("scheduler_native_id") != scheduler_job_id
            or connector_base.get("scheduler_step_marker") != step_marker
            or connector_base.get("connector_generation_id") != generation_id
            or connector_base.get("owner_token") != intent.get("owner_token")
            or connector_base.get("placement") != intent.get("placement")
            or _primitives._optional_str(connector_base.get("config_path")) is None
            or _primitives._optional_str(connector_base.get("log_path")) is None
            or connector_base.get("pid") is not None
        ):
            raise RelayError("allocation connector sidecar identity does not match its intent")
        record = _scheduler_contracts._last_json_object(
            self._ssh(
                _connector_step_scripts._remote_connector_step_reconcile_script(
                    definition=self.definition,
                    provider=provider_name,
                    scheduler_job_id=scheduler_job_id,
                    step_marker=step_marker,
                    placement_host=placement.placement_host,
                )
            )
        )
        if (
            record.get("schema_version") != "clio-relay.scheduler-connector-step-reconciliation.v1"
            or record.get("scheduler") != provider_name
            or record.get("scheduler_job_id") != scheduler_job_id
            or record.get("step_marker") != step_marker
            or record.get("placement_host") != placement.placement_host
            or not isinstance(record.get("found"), bool)
        ):
            raise RelayError("scheduler step reconciliation returned mismatched identity")
        if record.get("found") is False:
            if record.get("step") is not None:
                raise RelayError("scheduler step reconciliation contradicted step absence")
            return None, True
        if connector_base is None:
            raise RelayError("active scheduler connector step has no durable allocation sidecar")
        try:
            step = SchedulerConnectorStepIdentity.model_validate_json(
                json.dumps(record.get("step"), separators=(",", ":"), allow_nan=False)
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("scheduler step reconciliation returned invalid identity") from exc
        connector = {
            **connector_base,
            "scheduler_step_id": step.scheduler_step_id,
            "scheduler_step": step.model_dump(mode="json"),
        }
        self._allocation_connector_identity(
            session_id=session_id,
            connector=connector,
        )
        status = self._poll_allocation_connector_step(step)
        if status.state == "absent":
            return None, True
        return connector, False

    def _verified_scheduler_submission(
        self,
        session: GatewaySession,
        *,
        allow_quiesced_owner_source_recovery: bool = False,
    ) -> _types._VerifiedSchedulerSubmission:
        """Prove the exact provider and job ID from the relay-created remote sidecar."""
        scheduler_job_id = _primitives._optional_str(session.scheduler_job_id)
        if scheduler_job_id is None:
            raise RelayError("scheduler ownership verification requires an exact job id")
        try:
            spec = ServiceRuntimeSpec.model_validate(session.gateway.get("runtime_spec"))
        except ValueError as exc:
            raise RelayError("owned runtime has no valid service runtime specification") from exc
        binding_document = session.gateway.get("jarvis_runtime_binding")
        if binding_document is not None:
            try:
                verified = reverify_jarvis_service_runtime(
                    queue=self.queue,
                    definition=self.definition,
                    settings=self.settings,
                    binding_document=binding_document,
                )
            except (ConfigurationError, RelayError):
                if not (
                    allow_quiesced_owner_source_recovery
                    and self._quiesced_owner_source_recovery_is_authorized(session)
                ):
                    raise
                try:
                    verified = reverify_jarvis_service_runtime(
                        queue=self.queue,
                        definition=self.definition,
                        settings=None,
                        binding_document=binding_document,
                    )
                except ValueError as exc:
                    raise RelayError(
                        f"JARVIS service runtime binding re-verification failed: {exc}"
                    ) from exc
            except ValueError as exc:
                raise RelayError(
                    f"JARVIS service runtime binding re-verification failed: {exc}"
                ) from exc
            binding = verified.binding
            if (
                binding.scheduler_provider is None
                or binding.scheduler_native_id is None
                or binding.scheduler_provider != session.scheduler
                or binding.scheduler_native_id != scheduler_job_id
                or spec.scheduler != session.scheduler
            ):
                raise RelayError(
                    "scheduler identity disagrees with the verified JARVIS runtime binding"
                )
            return _types._VerifiedSchedulerSubmission(
                provider=binding.scheduler_provider,
                scheduler_job_id=binding.scheduler_native_id,
                spec=spec,
            )
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        scheduler_intent = _primitives._object(intents.get("scheduler_submission", {}))
        if (
            scheduler_intent.get("schema_version") != _primitives._OWNERSHIP_INTENT_SCHEMA
            or scheduler_intent.get("state") != "recorded"
        ):
            raise RelayError(
                "scheduler ownership is not backed by a recorded relay submission intent"
            )
        submission_id = _primitives._optional_str(scheduler_intent.get("submission_id"))
        intent_provider = _primitives._optional_str(scheduler_intent.get("scheduler_provider"))
        submission_marker = _primitives._optional_str(scheduler_intent.get("submission_marker"))
        intent_job_id = _primitives._optional_str(scheduler_intent.get("scheduler_job_id"))
        if None in {
            submission_id,
            intent_provider,
            submission_marker,
            intent_job_id,
        }:
            raise RelayError("recorded scheduler ownership intent has incomplete identity")
        assert submission_id is not None
        assert intent_provider is not None
        assert submission_marker is not None
        assert intent_job_id is not None
        try:
            canonical_provider = provider_for_scheduler(session.scheduler).name
        except ConfigurationError as exc:
            raise RelayError(f"scheduler provider identity is invalid: {exc}") from exc
        if (
            session.scheduler != canonical_provider
            or intent_provider != canonical_provider
            or spec.scheduler != canonical_provider
        ):
            raise RelayError(
                "scheduler provider identity disagrees between the runtime, "
                "submission intent, and runtime specification"
            )
        if intent_job_id != scheduler_job_id:
            raise RelayError(
                "scheduler job identity disagrees between the gateway and submission intent"
            )
        record = _scheduler_contracts._last_json_object(
            self._ssh(
                _submission_scripts._remote_submission_record_script(
                    session_id=session.session_id,
                    submission_id=submission_id,
                    scheduler_provider=intent_provider,
                    submission_marker=submission_marker,
                )
            )
        )
        output = record.get("output")
        if (
            record.get("schema_version") != "clio-relay.gateway-submission-sidecar.v1"
            or record.get("present") is not True
            or record.get("session_id") != session.session_id
            or record.get("submission_id") != submission_id
            or record.get("scheduler_provider") != canonical_provider
            or record.get("submission_marker") != submission_marker
            or record.get("returncode") != 0
            or record.get("output_truncated") is True
            or not isinstance(output, str)
        ):
            raise RelayError("scheduler submission sidecar identity is invalid")
        submission = _scheduler_contracts._parse_runtime_submission(output)
        if submission.scheduler_job_id != scheduler_job_id:
            raise RelayError("scheduler job identity disagrees with the anchored submission output")
        return _types._VerifiedSchedulerSubmission(
            provider=canonical_provider,
            scheduler_job_id=scheduler_job_id,
            spec=spec,
        )

    def _quiesced_owner_source_recovery_is_authorized(
        self,
        session: GatewaySession,
    ) -> bool:
        """Authorize a non-canceling direct source read for an exact closing owner."""
        teardown_intent = _primitives._object(session.gateway.get("teardown_intent", {}))
        owner_session_id = _primitives._optional_str(session.metadata.get("owner_session_id"))
        generation_id = _primitives._optional_str(
            session.metadata.get("owner_session_generation_id")
        )
        admission_id = _primitives._optional_str(session.metadata.get("owner_session_admission_id"))
        if owner_session_id is None or generation_id is None or admission_id is None:
            return False
        try:
            expected_admission_id = desktop_owner_session_admission_id(
                cluster=self.cluster,
                session_id=owner_session_id,
            )
        except ValueError:
            return False
        if (
            teardown_intent.get("schema_version") != "clio-relay.gateway-teardown-intent.v1"
            or teardown_intent.get("gateway_session_id") != session.session_id
            or teardown_intent.get("cancel_scheduler_job") is not False
            or self.settings.owner_session_id != owner_session_id
            or self.settings.owner_session_generation_id != generation_id
            or self.settings.resolved_owner_session_cluster() != self.cluster
            or admission_id != expected_admission_id
        ):
            return False
        try:
            cleanup_intent = self.queue.get_owner_session_cleanup_intent(
                admission_id,
                session_generation_id=generation_id,
            )
        except (OSError, QueueConflictError, ValueError):
            return False
        return bool(
            cleanup_intent is not None
            and cleanup_intent.get("schema_version") == "clio-relay.owner-session-cleanup-intent.v1"
            and cleanup_intent.get("owner_session_id") == admission_id
            and cleanup_intent.get("session_generation_id") == generation_id
            and cleanup_intent.get("cancel_scheduler_jobs") is False
            and isinstance(cleanup_intent.get("operation_id"), str)
            and bool(cleanup_intent.get("operation_id"))
        )

    def _stop_local_connector(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
        require_record: bool = False,
        absence_verified: bool = False,
    ) -> tuple[int | None, CleanupResource]:
        pid = _primitives._optional_int(connector.get("pid"))
        config_path = _primitives._optional_str(connector.get("config_path"))
        expected_directory = (
            self.settings.core_dir.parent / "runtime-sessions" / session_id
        ).resolve()
        config_owned = False
        if config_path is not None:
            try:
                config_owned = Path(config_path).resolve().parent == expected_directory
            except OSError:
                config_owned = False
        owned = (
            connector.get("owner") == "clio-relay"
            and connector.get("session_id") == session_id
            and config_owned
        )
        resource_id = str(pid) if pid is not None else session_id
        identity_status, identity_detail = _connector_identity._local_connector_identity_status(
            connector
        )
        if pid is None:
            residual = require_record and not absence_verified
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=absence_verified,
                outcome="refused" if residual else "missing",
                verified_after_operation=absence_verified,
                residual=residual,
                detail=(
                    "owned desktop connector record is missing"
                    if residual
                    else "no desktop connector was recorded"
                ),
            )
        if identity_status in {"missing", "replaced"}:
            try:
                no_group_members = not _connector_identity._local_connector_group_members(connector)
            except RelayError as exc:
                return None, CleanupResource(
                    kind="desktop_connector",
                    resource_id=resource_id,
                    location="desktop",
                    action="stop",
                    ownership_verified=False,
                    outcome="failed",
                    residual=True,
                    detail=str(exc),
                )
            durable_identity = (
                owned
                and _primitives._optional_str(connector.get("owner_token")) is not None
                and _primitives._optional_int(connector.get("process_group_id")) is not None
                and _primitives._optional_str(connector.get("process_start_marker")) is not None
                and no_group_members
            )
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=durable_identity,
                outcome="missing" if durable_identity else "refused",
                verified_after_operation=durable_identity,
                residual=not durable_identity,
                detail=identity_detail,
            )
        if not owned or identity_status != "owned":
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=False,
                outcome="refused",
                residual=True,
                detail=identity_detail
                or "connector process does not match the owned session record",
            )
        try:
            stopped = _connector_identity._terminate_local_connector(connector)
            residual = bool(_connector_identity._local_connector_group_members(connector))
        except RelayError as exc:
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=False,
                outcome="failed",
                residual=True,
                detail=str(exc),
            )
        return stopped, CleanupResource(
            kind="desktop_connector",
            resource_id=resource_id,
            location="desktop",
            action="stop",
            ownership_verified=True,
            outcome="failed" if residual else "stopped",
            verified_after_operation=not residual,
            residual=residual,
            detail="connector still running after termination" if residual else None,
        )

    def _remove_unpublished_local_connector_files(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> None:
        """Remove private files for a connector that failed before durable publication."""

        expected_directory = (
            self.settings.core_dir.parent / "runtime-sessions" / session_id
        ).resolve()
        paths: list[Path] = []
        for field in ("config_path", "stdout_path", "stderr_path", "metadata_path"):
            raw_path = _primitives._optional_str(connector.get(field))
            if raw_path is None:
                raise RelayError(f"unpublished desktop connector omitted {field}")
            path = Path(raw_path).resolve()
            if path.parent != expected_directory:
                raise RelayError("unpublished desktop connector path escaped its runtime directory")
            paths.append(path)
        try:
            for path in paths:
                path.unlink(missing_ok=True)
        except OSError as exc:
            raise RelayError("could not remove unpublished desktop connector files") from exc

    def _observe_allocation_and_health_once(
        self,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
        initial_service_host: str | None = None,
    ) -> str | None:
        """Make one bounded scheduler/runtime/health observation without waiting."""

        current_session = session
        provider_status: SchedulerStatus | None = None
        try:
            provider_status = (
                self._poll_scheduler_provider(
                    provider=spec.scheduler,
                    scheduler_job_id=scheduler_job_id,
                )
                if provider_for_scheduler(spec.scheduler).name != "external"
                else None
            )
        except ConfigurationError:
            raise
        except RelayError as exc:
            self._record_runtime_observation_pending(
                current_session,
                node=initial_service_host or current_session.node,
                error=exc,
                provider_status=None,
            )
            return None
        if provider_status is not None:
            provider_state = provider_status.phase.value
            if provider_state in _scheduler_contracts._TERMINAL_RUNTIME_STATES:
                raise _types._DefinitiveRuntimeObservationError(
                    "scheduler job reached a terminal state before the service became ready: "
                    f"job={scheduler_job_id} state={provider_state}"
                )
            if provider_status.phase is SchedulerPhase.UNKNOWN:
                self._record_runtime_observation_pending(
                    current_session,
                    node=initial_service_host or current_session.node,
                    error=RelayError(
                        "scheduler provider could not observe a current or terminal record for "
                        f"submitted job {scheduler_job_id}; absence is not terminal proof"
                    ),
                    provider_status=provider_status,
                )
                return None
            if provider_status.phase in {SchedulerPhase.SUBMITTED, SchedulerPhase.PENDING}:
                observed_gateway = dict(current_session.gateway)
                observed_gateway.pop("runtime_observation", None)
                self._update(
                    current_session,
                    state=GatewaySessionState.PENDING,
                    queue_state=provider_state,
                    node=None,
                    metadata={
                        "runtime_observation_error": None,
                        "runtime_observed_at": utc_now().isoformat(),
                    },
                    gateway={
                        **observed_gateway,
                        "scheduler_status": {
                            "raw": provider_status.model_dump_json(),
                            "state": provider_state,
                            "reason": provider_status.reason,
                            "provider": provider_status.model_dump(mode="json"),
                        },
                    },
                )
                return None
        try:
            if initial_service_host is not None:
                scheduler_state = (
                    provider_status.phase.value if provider_status is not None else "allocated"
                )
                node = initial_service_host
                reason = provider_status.reason if provider_status is not None else None
                runtime_events: list[dict[str, object]] | None = None
                status_text = json.dumps(
                    {
                        "scheduler_job_id": scheduler_job_id,
                        "service_host": initial_service_host,
                    },
                    sort_keys=True,
                )
            else:
                if spec.status_command is None:
                    raise ConfigurationError(
                        "service host was not reported by submission output; "
                        "ServiceRuntimeSpec.status_command is required"
                    )
                status_text = self._ssh(
                    _submission_scripts._template_command_script(
                        spec.status_command, scheduler_job_id
                    )
                )
                status = _scheduler_contracts._parse_runtime_status(status_text)
                scheduler_state = (
                    provider_status.phase.value
                    if provider_status is not None
                    else status.state or "unknown"
                )
                node = status.service_host
                reason = (
                    provider_status.reason
                    if provider_status is not None and provider_status.reason is not None
                    else status.reason
                )
                runtime_events = status.events
        except ConfigurationError:
            raise
        except RelayError as exc:
            self._record_runtime_observation_pending(
                current_session,
                node=initial_service_host or current_session.node,
                error=exc,
                provider_status=provider_status,
            )
            return None

        last_status = status_text.strip()
        normalized_scheduler_state = (
            scheduler_state.strip().lower() if scheduler_state else "unknown"
        )
        if normalized_scheduler_state in _scheduler_contracts._TERMINAL_RUNTIME_STATES:
            raise _types._DefinitiveRuntimeObservationError(
                "scheduler job reached a terminal state before the service became ready: "
                f"job={scheduler_job_id} state={normalized_scheduler_state}"
            )
        state = GatewaySessionState.ALLOCATED if node is not None else GatewaySessionState.PENDING
        observed_gateway = dict(current_session.gateway)
        observed_gateway.pop("runtime_observation", None)
        current_session = self._update(
            current_session,
            state=state,
            queue_state=normalized_scheduler_state,
            node=node,
            metadata={
                "runtime_observation_error": None,
                "runtime_observed_at": utc_now().isoformat(),
            },
            gateway={
                **observed_gateway,
                "scheduler_status": {
                    "raw": last_status,
                    "state": scheduler_state,
                    "reason": reason,
                    "provider": (
                        provider_status.model_dump(mode="json")
                        if provider_status is not None
                        else None
                    ),
                },
                "runtime_events": runtime_events or [],
            },
        )
        if node is None:
            return None
        try:
            health = self._ssh(
                _connector_step_scripts._remote_http_probe_script(
                    node,
                    spec.service_port,
                    spec.health_path,
                    expected_body=spec.health_expected_body,
                )
            )
        except RelayError as exc:
            self._record_runtime_observation_pending(
                current_session,
                node=node,
                error=exc,
                provider_status=provider_status,
            )
            return None
        if "service_health=ok" in health:
            return node
        self._record_runtime_observation_pending(
            current_session,
            node=node,
            error=RelayError(
                "service health observation was not ready: "
                f"job={scheduler_job_id} output={health.strip()!r}"
            ),
            provider_status=provider_status,
        )
        return None

    def _record_runtime_observation_pending(
        self,
        session: GatewaySession,
        *,
        node: str | None,
        error: RelayError,
        provider_status: SchedulerStatus | None,
        state: GatewaySessionState | None = None,
        queue_state: str = "observation_unknown",
        preserve_scheduler_status: bool = False,
    ) -> GatewaySession:
        """Persist an inconclusive observation without failing the owned submission."""

        previous_status = _primitives._object(session.gateway.get("scheduler_status", {}))
        observed_at = utc_now().isoformat()
        gateway = {
            **session.gateway,
            "runtime_observation": {
                "state": "not_ready",
                "error": str(error),
                "observed_at": observed_at,
            },
        }
        if not preserve_scheduler_status:
            gateway["scheduler_status"] = {
                "raw": previous_status.get("raw", ""),
                "state": "observation_unknown",
                "reason": str(error),
                "provider": (
                    provider_status.model_dump(mode="json")
                    if provider_status is not None
                    else previous_status.get("provider")
                ),
            }
        return self._update(
            session,
            state=state
            or (GatewaySessionState.ALLOCATED if node is not None else GatewaySessionState.PENDING),
            queue_state=queue_state,
            node=node,
            metadata={
                "runtime_observation_error": str(error),
                "runtime_observed_at": observed_at,
            },
            gateway=gateway,
        )

    def _retained_scheduler_resource(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
    ) -> CleanupResource:
        scheduler_job_id = session.scheduler_job_id
        if scheduler_job_id is None:
            raise ConfigurationError("scheduler retention requires a scheduler job id")
        try:
            provider = provider_for_scheduler(session.scheduler)
            if provider.name == "external":
                observed_state = self._observe_runtime_state(
                    spec=spec,
                    scheduler_job_id=scheduler_job_id,
                )
            else:
                provider_status = self._poll_scheduler_provider(
                    provider=provider.name,
                    scheduler_job_id=scheduler_job_id,
                )
                observed_state = provider_status.phase.value
        except RelayError as exc:
            return CleanupResource(
                kind="scheduler_job",
                resource_id=scheduler_job_id,
                location=self.definition.ssh_host,
                provider=session.scheduler,
                action="retain",
                metadata={"gateway_session_id": session.session_id},
                ownership_verified=True,
                outcome="failed",
                verified_after_operation=False,
                residual=True,
                detail=(
                    "scheduler cancellation was not requested, but retained-state "
                    f"verification failed: {exc}"
                ),
            )
        if observed_state in {"missing", "not-found", "not_found", "unknown"}:
            return CleanupResource(
                kind="scheduler_job",
                resource_id=scheduler_job_id,
                location=self.definition.ssh_host,
                provider=session.scheduler,
                action="retain",
                metadata={"gateway_session_id": session.session_id},
                ownership_verified=True,
                outcome="failed",
                verified_after_operation=False,
                observed_state=observed_state,
                residual=True,
                detail=(
                    "scheduler cancellation was not requested, but retained-state "
                    f"verification remained unresolved: {observed_state}"
                ),
            )
        scheduler_terminal = observed_state in _scheduler_contracts._TERMINAL_RUNTIME_STATES
        return CleanupResource(
            kind="scheduler_job",
            resource_id=scheduler_job_id,
            location=self.definition.ssh_host,
            provider=session.scheduler,
            action="retain",
            metadata={"gateway_session_id": session.session_id},
            ownership_verified=True,
            outcome="terminal" if scheduler_terminal else "retained",
            verified_after_operation=True,
            observed_state=observed_state,
            detail=(
                "scheduler cancellation was not requested; observed "
                f"{'terminal' if scheduler_terminal else 'active'} runtime state: "
                f"{observed_state}"
            ),
        )

    def _observe_runtime_state(
        self,
        *,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
    ) -> str:
        if spec.status_command is None:
            raise RelayError("runtime status command is required for retained-state verification")
        status_text = self._ssh(
            _submission_scripts._template_command_script(spec.status_command, scheduler_job_id)
        )
        status = _scheduler_contracts._parse_runtime_status(status_text)
        if status.state is None or not status.state.strip():
            raise RelayError(
                f"runtime status did not report a state for scheduler job {scheduler_job_id}"
            )
        normalized = status.state.strip().lower()
        if (
            normalized
            not in _scheduler_contracts._ACTIVE_RUNTIME_STATES
            | _scheduler_contracts._TERMINAL_RUNTIME_STATES
        ):
            raise RelayError(
                "runtime status reported an unsupported state for scheduler job "
                f"{scheduler_job_id}: {normalized}"
            )
        return normalized

    def _observe_scheduler_state(
        self,
        *,
        scheduler: str,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
    ) -> str:
        provider = provider_for_scheduler(scheduler)
        if provider.name == "external":
            return self._observe_runtime_state(
                spec=spec,
                scheduler_job_id=scheduler_job_id,
            )
        return self._poll_scheduler_provider(
            provider=provider.name,
            scheduler_job_id=scheduler_job_id,
        ).phase.value

    def _wait_for_scheduler_terminal(
        self,
        *,
        scheduler: str,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
    ) -> str:
        deadline = time.time() + spec.readiness_timeout_seconds
        last_state = "unknown"
        while time.time() < deadline:
            last_state = self._observe_scheduler_state(
                scheduler=scheduler,
                spec=spec,
                scheduler_job_id=scheduler_job_id,
            )
            if last_state in _scheduler_contracts._TERMINAL_RUNTIME_STATES:
                return last_state
            self.sleep(spec.poll_seconds)
        raise RelayError(
            "runtime cancellation was not confirmed terminal before timeout: "
            f"job={scheduler_job_id} last_state={last_state}"
        )

    def _poll_scheduler_provider(
        self,
        *,
        provider: str,
        scheduler_job_id: str,
    ) -> SchedulerStatus:
        output = self._ssh(
            _submission_scripts._remote_scheduler_script(
                definition=self.definition,
                operation="status",
                provider=provider,
                scheduler_job_id=scheduler_job_id,
            )
        )
        try:
            status = SchedulerStatus.model_validate(_scheduler_contracts._last_json_object(output))
        except (ValueError, TypeError) as exc:
            raise RelayError("scheduler provider returned invalid structured status") from exc
        expected_provider = provider_for_scheduler(provider).name
        if status.scheduler != expected_provider:
            raise RelayError(
                "scheduler provider identity mismatch: "
                f"{status.scheduler!r} != {expected_provider!r}"
            )
        if status.scheduler_job_id != scheduler_job_id:
            raise RelayError(
                "scheduler provider job identity mismatch: "
                f"{status.scheduler_job_id!r} != {scheduler_job_id!r}"
            )
        return status

    def _request_scheduler_provider_cancel(
        self,
        *,
        provider: str,
        scheduler_job_id: str,
    ) -> None:
        output = self._ssh(
            _submission_scripts._remote_scheduler_script(
                definition=self.definition,
                operation="cancel",
                provider=provider,
                scheduler_job_id=scheduler_job_id,
            )
        )
        result = _scheduler_contracts._last_json_object(output)
        if (
            result.get("scheduler") != provider_for_scheduler(provider).name
            or result.get("scheduler_job_id") != scheduler_job_id
            or result.get("cancel_requested") is not True
            or result.get("accepted") is not True
            or result.get("returncode") != 0
        ):
            raise RelayError("scheduler provider did not accept exact-job cancellation")

    def _start_remote_connector(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        node: str,
        proxy_name: str,
        ownership_intent: dict[str, object],
        allocation_provider: str | None = None,
        allocation_job_id: str | None = None,
    ) -> dict[str, object]:
        if (allocation_provider is None) != (allocation_job_id is None):
            raise ConfigurationError(
                "allocation_provider and allocation_job_id must be provided together"
            )
        placement: SchedulerConnectorPlacement | None = None
        step_marker: str | None = None
        if allocation_provider is not None and allocation_job_id is not None:
            provider = provider_for_scheduler(allocation_provider)
            if not isinstance(provider, SchedulerAllocationConnectorProvider):
                raise ConfigurationError(
                    f"scheduler provider {allocation_provider!r} cannot launch an "
                    "allocation-scoped connector"
                )
            raw_placement = _scheduler_contracts._last_json_object(
                self._ssh(
                    _submission_scripts._remote_scheduler_script(
                        definition=self.definition,
                        operation="connector-placement",
                        provider=allocation_provider,
                        scheduler_job_id=allocation_job_id,
                    )
                )
            )
            try:
                placement = SchedulerConnectorPlacement.model_validate_json(
                    json.dumps(raw_placement, separators=(",", ":"), allow_nan=False)
                )
            except ValueError as exc:
                raise RelayError(
                    "scheduler provider returned invalid connector placement evidence"
                ) from exc
            if (
                placement.scheduler != allocation_provider
                or placement.scheduler_job_id != allocation_job_id
                or placement.allocation_node_count != 1
                or placement.verified is not True
            ):
                raise RelayError("scheduler connector placement identity did not match binding")
            step_marker = _scheduler_contracts._connector_step_marker(
                session.session_id,
                _scheduler_contracts._required_intent_str(
                    ownership_intent,
                    "connector_generation_id",
                ),
            )
            ownership_intent = _scheduler_contracts._new_ownership_intent(
                "starting",
                owner_token=_scheduler_contracts._required_intent_str(
                    ownership_intent, "owner_token"
                ),
                connector_generation_id=_scheduler_contracts._required_intent_str(
                    ownership_intent,
                    "connector_generation_id",
                ),
                execution_scope="scheduler_allocation",
                scheduler_provider=allocation_provider,
                scheduler_native_id=allocation_job_id,
                scheduler_step_marker=step_marker,
                placement=placement.model_dump(mode="json"),
            )
            # Persist the allocation, placement, and unique step marker before
            # A detached ``srun`` can create a scheduler-side process.
            self._set_ownership_intent(
                session,
                "remote_connector",
                ownership_intent,
            )
        transport = self.definition.frp_transport
        server_addr = _primitives._require_server_addr(transport.server_addr, self.cluster)
        config = render_proxy_config(
            FrpLinkConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                protocol=FrpTransportProtocol(transport.protocol),
                token=self.token,
                secret_key=self.secret_key,
                proxy_name=proxy_name,
            ),
            proxy_type=_primitives._frp_proxy_type(spec.transport_mode),
            local_ip=node,
            local_port=spec.service_port,
        )
        owner_token = _scheduler_contracts._required_intent_str(ownership_intent, "owner_token")
        connector_generation_id = _scheduler_contracts._required_intent_str(
            ownership_intent,
            "connector_generation_id",
        )
        if allocation_provider is not None and allocation_job_id is not None:
            if placement is None or step_marker is None:
                raise AssertionError("allocation placement and step marker were not resolved")
            output = self._ssh(
                _remote_allocation_frpc_start_script(
                    definition=self.definition,
                    session_id=session.session_id,
                    config_text=config,
                    owner_token=owner_token,
                    connector_generation_id=connector_generation_id,
                    allocation_provider=allocation_provider,
                    allocation_job_id=allocation_job_id,
                    placement=placement,
                    step_marker=step_marker,
                )
            )
            start_result = _scheduler_contracts._last_json_object(output)
            if start_result.get("schema_version") != "clio-relay.allocation-connector-start.v1":
                raise RelayError("allocation connector start returned the wrong schema")
            if (
                start_result.get("session_id") != session.session_id
                or start_result.get("connector_generation_id") != connector_generation_id
            ):
                raise RelayError("allocation connector start identity did not match its intent")
            raw_step = start_result.get("step_identity")
            try:
                step_identity = SchedulerConnectorStepIdentity.model_validate_json(
                    json.dumps(raw_step, separators=(",", ":"), allow_nan=False)
                )
            except (TypeError, ValueError) as exc:
                raise RelayError(
                    "allocation connector start returned invalid scheduler step identity"
                ) from exc
            if (
                step_identity.scheduler != allocation_provider
                or step_identity.scheduler_job_id != allocation_job_id
                or step_identity.placement_host != placement.placement_host
                or step_identity.step_marker != step_marker
                or step_identity.verified is not True
            ):
                raise RelayError("allocation connector scheduler step identity did not match")
            config_path = _primitives._optional_str(start_result.get("config_path"))
            log_path = _primitives._optional_str(start_result.get("log_path"))
            if config_path is None or log_path is None:
                raise RelayError("allocation connector start omitted its owned paths")
            return {
                "owner": "clio-relay",
                "session_id": session.session_id,
                "execution_scope": "scheduler_allocation",
                "scheduler_provider": allocation_provider,
                "scheduler_native_id": allocation_job_id,
                "scheduler_step_id": step_identity.scheduler_step_id,
                "scheduler_step_marker": step_marker,
                "scheduler_step": step_identity.model_dump(mode="json"),
                "connector_generation_id": connector_generation_id,
                "owner_token": owner_token,
                "config_path": config_path,
                "log_path": log_path,
                "placement": placement.model_dump(mode="json"),
            }
        output = self._ssh(
            _remote_frpc_start_script(
                definition=self.definition,
                session_id=session.session_id,
                config_text=config,
                owner_token=owner_token,
                connector_generation_id=connector_generation_id,
            )
        )
        metadata = _scheduler_contracts._key_value_output(output)
        expected_fields = {
            "remote_frpc_pid",
            "remote_frpc_pgid",
            "connector_generation_id",
            "remote_frpc_config",
            "remote_frpc_log",
        }
        if set(metadata) != expected_fields:
            raise RelayError("remote connector start returned an invalid response shape")
        try:
            pid = int(metadata["remote_frpc_pid"])
            process_group_id = int(metadata["remote_frpc_pgid"])
        except ValueError as exc:
            raise RelayError("remote connector start returned an invalid process identity") from exc
        if pid <= 0 or process_group_id != pid:
            raise RelayError("remote connector start returned an invalid process group identity")
        if metadata["connector_generation_id"] != connector_generation_id:
            raise RelayError("remote connector start identity did not match its durable intent")
        config_path = _scheduler_contracts._validated_remote_session_file(
            metadata["remote_frpc_config"],
            session_id=session.session_id,
            filename="remote-frpc.toml",
        )
        log_path = _scheduler_contracts._validated_remote_session_file(
            metadata["remote_frpc_log"],
            session_id=session.session_id,
            filename="remote-frpc.log",
        )
        if config_path.parent != log_path.parent:
            raise RelayError("remote connector start returned paths from different sessions")
        connector: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": pid,
            "process_group_id": process_group_id,
            "connector_generation_id": connector_generation_id,
            "owner_token": owner_token,
            "config_path": config_path.as_posix(),
            "log_path": log_path.as_posix(),
        }
        return connector

    def _allocation_connector_identity(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> SchedulerConnectorStepIdentity:
        """Validate exact provider, allocation, placement, and step ownership."""
        if (
            connector.get("owner") != "clio-relay"
            or connector.get("session_id") != session_id
            or connector.get("execution_scope") != "scheduler_allocation"
            or connector.get("pid") is not None
            or connector.get("process_group_id") is not None
        ):
            raise RelayError("allocation connector ownership record is invalid")
        try:
            step = SchedulerConnectorStepIdentity.model_validate_json(
                json.dumps(
                    connector.get("scheduler_step"),
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
            placement = SchedulerConnectorPlacement.model_validate_json(
                json.dumps(
                    connector.get("placement"),
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("allocation connector has invalid provider-native identity") from exc
        generation_id = _primitives._optional_str(connector.get("connector_generation_id"))
        provider_name = _primitives._optional_str(connector.get("scheduler_provider"))
        scheduler_job_id = _primitives._optional_str(connector.get("scheduler_native_id"))
        scheduler_step_id = _primitives._optional_str(connector.get("scheduler_step_id"))
        step_marker = _primitives._optional_str(connector.get("scheduler_step_marker"))
        config_path = _primitives._optional_str(connector.get("config_path"))
        log_path = _primitives._optional_str(connector.get("log_path"))
        if None in {
            generation_id,
            provider_name,
            scheduler_job_id,
            scheduler_step_id,
            step_marker,
            config_path,
            log_path,
        }:
            raise RelayError("allocation connector ownership record is incomplete")
        assert generation_id is not None
        assert provider_name is not None
        assert scheduler_job_id is not None
        assert scheduler_step_id is not None
        assert step_marker is not None
        try:
            provider = provider_for_scheduler(provider_name)
        except ConfigurationError as exc:
            raise RelayError(f"allocation connector provider is invalid: {exc}") from exc
        if not isinstance(provider, SchedulerAllocationConnectorProvider):
            raise RelayError("allocation connector provider lacks step lifecycle semantics")
        if (
            provider.name != provider_name
            or step.scheduler != provider_name
            or step.scheduler_job_id != scheduler_job_id
            or step.scheduler_step_id != scheduler_step_id
            or step.step_marker != step_marker
            or step_marker != _scheduler_contracts._connector_step_marker(session_id, generation_id)
            or placement.scheduler != provider_name
            or placement.scheduler_job_id != scheduler_job_id
            or placement.placement_host != step.placement_host
            or placement.allocation_node_count != 1
            or step.verified is not True
            or placement.verified is not True
        ):
            raise RelayError("allocation connector identities disagree")
        return step

    def _poll_allocation_connector_step(
        self,
        identity: SchedulerConnectorStepIdentity,
    ) -> SchedulerConnectorStepStatus:
        """Poll one exact provider-native connector step over the cluster boundary."""
        output = self._ssh(
            _connector_step_scripts._remote_connector_step_status_script(
                definition=self.definition,
                provider=identity.scheduler,
                scheduler_job_id=identity.scheduler_job_id,
                scheduler_step_id=identity.scheduler_step_id,
                placement_host=identity.placement_host,
            )
        )
        try:
            status = SchedulerConnectorStepStatus.model_validate_json(
                json.dumps(
                    _scheduler_contracts._last_json_object(output),
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("scheduler returned invalid connector step status") from exc
        if (
            status.scheduler != identity.scheduler
            or status.scheduler_job_id != identity.scheduler_job_id
            or status.scheduler_step_id != identity.scheduler_step_id
            or status.placement_host != identity.placement_host
            or status.verified is not True
        ):
            raise RelayError("scheduler connector step status identity did not match")
        return status

    def _stop_allocation_connector(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> CleanupResource:
        """Cancel one exact scheduler step and prove its compute-node absence."""
        identity = self._allocation_connector_identity(
            session_id=session_id,
            connector=connector,
        )
        status = self._poll_allocation_connector_step(identity)
        cancel_error: str | None = None
        canceled = False
        if status.state == "active":
            try:
                result = _scheduler_contracts._last_json_object(
                    self._ssh(
                        _connector_step_scripts._remote_connector_step_cancel_script(
                            definition=self.definition,
                            provider=identity.scheduler,
                            scheduler_job_id=identity.scheduler_job_id,
                            scheduler_step_id=identity.scheduler_step_id,
                        )
                    )
                )
                if (
                    result.get("scheduler") != identity.scheduler
                    or result.get("scheduler_job_id") != identity.scheduler_job_id
                    or result.get("scheduler_step_id") != identity.scheduler_step_id
                    or result.get("cancel_requested") is not True
                    or result.get("accepted") is not True
                    or result.get("returncode") != 0
                ):
                    raise RelayError("scheduler did not accept exact connector-step cancellation")
                canceled = True
            except RelayError as exc:
                cancel_error = str(exc)
            attempts = max(
                1,
                math.ceil(
                    _CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS / _CONNECTOR_STEP_CLEANUP_POLL_SECONDS
                ),
            )
            for attempt in range(attempts):
                status = self._poll_allocation_connector_step(identity)
                if status.state == "absent":
                    break
                if attempt + 1 < attempts:
                    self.sleep(_CONNECTOR_STEP_CLEANUP_POLL_SECONDS)
        if status.state != "absent":
            detail = "scheduler connector step remains active after exact-step cancellation"
            if cancel_error is not None:
                detail = f"{detail}: {cancel_error}"
            raise RelayError(detail)
        return CleanupResource(
            kind="remote_connector",
            resource_id=identity.scheduler_step_id,
            location=identity.placement_host,
            provider=identity.scheduler,
            action="stop",
            ownership_verified=True,
            outcome="stopped" if canceled else "missing",
            verified_after_operation=True,
            observed_state="absent",
            detail=(
                "exact scheduler connector step absence confirmed"
                + (f" after cancellation error: {cancel_error}" if cancel_error else "")
            ),
            metadata={
                "scheduler_job_id": identity.scheduler_job_id,
                "scheduler_step_id": identity.scheduler_step_id,
                "scheduler_step_marker": identity.step_marker,
                "placement_host": identity.placement_host,
                "parent_scheduler_job_retained": True,
            },
        )

    def _retained_allocation_connector_resource(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> CleanupResource:
        """Prove that a detached allocation-scoped connector remains active."""
        identity = self._allocation_connector_identity(
            session_id=session_id,
            connector=connector,
        )
        status = self._poll_allocation_connector_step(identity)
        retained = status.state == "active"
        return CleanupResource(
            kind="remote_connector",
            resource_id=identity.scheduler_step_id,
            location=identity.placement_host,
            provider=identity.scheduler,
            action="retain",
            ownership_verified=True,
            outcome="retained" if retained else "failed",
            verified_after_operation=True,
            observed_state=status.state,
            residual=not retained,
            detail=(
                "exact scheduler connector step retained for reattachment"
                if retained
                else "scheduler confirms the allocation connector step is absent"
            ),
            metadata={
                "scheduler_job_id": identity.scheduler_job_id,
                "scheduler_step_id": identity.scheduler_step_id,
                "scheduler_step_marker": identity.step_marker,
                "placement_host": identity.placement_host,
                "parent_scheduler_job_retained": True,
            },
        )

    def _start_local_visitor(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        proxy_name: str,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        transport = self.definition.frp_transport
        server_addr = _primitives._require_server_addr(transport.server_addr, self.cluster)
        runtime_dir = self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        runtime_dir.mkdir(parents=True, exist_ok=True)
        config_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "config_path")
        ).resolve()
        stdout_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stdout_path")
        ).resolve()
        stderr_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stderr_path")
        ).resolve()
        metadata_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "metadata_path")
        ).resolve()
        owned_paths = (config_path, stdout_path, stderr_path, metadata_path)
        if any(path.parent != runtime_dir.resolve() for path in owned_paths):
            raise RelayError("desktop connector ownership intent escaped its runtime directory")
        owner_token = _scheduler_contracts._required_intent_str(ownership_intent, "owner_token")
        connector_generation_id = _scheduler_contracts._required_intent_str(
            ownership_intent,
            "connector_generation_id",
        )
        visitor_type = _primitives._frp_proxy_type(spec.transport_mode)
        visitor = start_owned_frp_visitor(
            frpc_bin=self.settings.frpc_bin,
            config=FrpLinkConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                protocol=FrpTransportProtocol(transport.protocol),
                token=self.token,
                secret_key=self.secret_key,
                proxy_name=proxy_name,
            ),
            local_bind_addr=spec.desktop_bind_addr,
            local_bind_port=spec.desktop_bind_port,
            visitor_type=visitor_type,
            keep_tunnel_open=visitor_type == "xtcp",
            config_path=config_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            owner_token=owner_token,
            connector_generation_id=connector_generation_id,
            command_prefix=[
                sys.executable,
                "-c",
                _LOCAL_CONNECTOR_WRAPPER_CODE,
                owner_token,
                connector_generation_id,
            ],
            process_factory=self.runner.popen,
            identity_factory=self.runner.local_process_identity,
            rollback=_primitives._terminate_just_started_process_group,
        )
        connector: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": visitor.pid,
            "process_group_id": visitor.process_group_id,
            "process_start_marker": visitor.process_start_marker,
            "owner_token": visitor.owner_token,
            "connector_generation_id": connector_generation_id,
            "config_path": str(visitor.config_path),
            "stdout_path": str(visitor.stdout_path),
            "stderr_path": str(visitor.stderr_path),
            "metadata_path": str(metadata_path),
        }
        _connector_identity._write_local_connector_sidecar(metadata_path, connector)
        return connector

    def _start_browser_proxy(
        self,
        *,
        session: GatewaySession,
        config: BrowserGatewayConfig,
        capability: str,
        upstream_authorization: str | None,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        """Start one owned capability proxy without placing either secret on disk."""
        runtime_dir = (
            self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        ).resolve()
        config_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "config_path")
        ).resolve()
        stdout_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stdout_path")
        ).resolve()
        stderr_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stderr_path")
        ).resolve()
        metadata_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "metadata_path")
        ).resolve()
        if any(
            path.parent != runtime_dir
            for path in (config_path, stdout_path, stderr_path, metadata_path)
        ):
            raise RelayError("browser proxy ownership intent escaped its runtime directory")
        temporary = config_path.with_suffix(f"{config_path.suffix}.{os.getpid()}.tmp")
        temporary.write_text(config.model_dump_json(indent=2) + "\n", encoding="utf-8")
        temporary.chmod(0o600)
        os.replace(temporary, config_path)
        owner_token = _scheduler_contracts._required_intent_str(ownership_intent, "owner_token")
        generation_id = _scheduler_contracts._required_intent_str(
            ownership_intent, "connector_generation_id"
        )
        environment = os.environ.copy()
        environment.pop(CAPABILITY_ENV, None)
        environment.pop(UPSTREAM_AUTHORIZATION_ENV, None)
        environment["CLIO_RELAY_CONNECTOR_OWNER_TOKEN"] = owner_token
        environment["CLIO_RELAY_CONNECTOR_GENERATION_ID"] = generation_id
        bootstrap = (
            BrowserGatewayBootstrap(
                capability=capability,
                upstream_authorization=upstream_authorization,
            )
            .model_dump_json()
            .encode("utf-8")
        )
        process = self.runner.popen(
            [
                sys.executable,
                "-c",
                _LOCAL_CONNECTOR_WRAPPER_CODE,
                owner_token,
                generation_id,
                sys.executable,
                "-m",
                "clio_relay.browser_gateway",
                "--config",
                str(config_path),
                "--process-label",
                "clio-relay-browser-frpc-proxy",
            ],
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            env=environment,
            isolate_process_group=True,
            input_bytes=bootstrap,
        )
        try:
            identity = self.runner.local_process_identity(
                pid=process.pid,
                owner_token=owner_token,
                expected_config=str(config_path),
            )
        except BaseException:
            _primitives._terminate_just_started_process_group(process.pid)
            raise
        proxy: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "attachment_id": config.attachment_id,
            "pid": process.pid,
            "process_group_id": identity.process_group_id,
            "process_start_marker": identity.process_start_marker,
            "owner_token": identity.owner_token,
            "connector_generation_id": generation_id,
            "config_path": str(config_path),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "metadata_path": str(metadata_path),
        }
        _connector_identity._write_local_connector_sidecar(metadata_path, proxy)
        return proxy

    def _wait_for_jarvis_health(
        self,
        health_url: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
        runtime_schema_version: Literal["jarvis.service-runtime.v1", "jarvis.service-runtime.v2"],
        authorization: str | None,
        max_attempts: int | None = None,
    ) -> None:
        """Prove the versioned JARVIS HTTP authorization boundary is live."""
        if runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
            if authorization is not None:
                raise _types._DefinitiveRuntimeObservationError(
                    "legacy JARVIS service runtime unexpectedly resolved authorization"
                )
        elif runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V2:
            if authorization is None:
                raise _types._DefinitiveRuntimeObservationError(
                    "authenticated JARVIS service runtime omitted authorization"
                )
        else:
            raise _types._DefinitiveRuntimeObservationError(
                "JARVIS service runtime schema is unsupported"
            )
        deadline = time.monotonic() + timeout_seconds
        last_error = "no response"
        attempts = 0
        while time.monotonic() < deadline:
            attempts += 1
            try:
                anonymous = _readiness._read_bounded_http_response(
                    health_url,
                    headers=None,
                    maximum_bytes=None,
                    deadline=deadline,
                )
                if runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
                    if 200 <= anonymous.status_code < 300:
                        return
                    last_error = f"legacy anonymous health status={anonymous.status_code}"
                else:
                    if 200 <= anonymous.status_code < 300:
                        raise _types._DefinitiveRuntimeObservationError(
                            "authenticated JARVIS service health accepted an anonymous request"
                        )
                    if anonymous.status_code != 401:
                        last_error = f"anonymous health status={anonymous.status_code}"
                    else:
                        authenticated = _readiness._read_bounded_http_response(
                            health_url,
                            headers={"Authorization": cast(str, authorization)},
                            maximum_bytes=None,
                            deadline=deadline,
                        )
                        if 200 <= authenticated.status_code < 300:
                            return
                        if authenticated.status_code in {401, 403}:
                            raise _types._DefinitiveRuntimeObservationError(
                                "authenticated JARVIS service rejected its verified authority"
                            )
                        last_error = f"authenticated health status={authenticated.status_code}"
            except httpx.HTTPError:
                last_error = "HTTP transport failed"
            if max_attempts is not None and attempts >= max_attempts:
                break
            _readiness._sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"JARVIS service health boundary was not ready: {last_error}")

    def _wait_for_browser_health(
        self,
        health_url: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> None:
        """Prove the capability proxy forwards health with exact sandbox-origin CORS."""
        deadline = time.monotonic() + timeout_seconds
        last_error = "no response"
        while time.monotonic() < deadline:
            try:
                response = _readiness._read_bounded_http_response(
                    health_url,
                    headers={"Origin": "null"},
                    maximum_bytes=None,
                    deadline=deadline,
                )
                if (
                    200 <= response.status_code < 300
                    and response.headers.get("access-control-allow-origin") == "null"
                ):
                    return
                last_error = (
                    f"status={response.status_code}; "
                    "access-control-allow-origin was not exactly null"
                )
            except httpx.HTTPError:
                last_error = "HTTP transport failed"
            _readiness._sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"browser capability gateway did not become ready: {last_error}")

    def _wait_for_local_health(
        self,
        health_url: str,
        timeout_seconds: float,
        poll_seconds: float,
        *,
        expected_body: str | None = None,
        max_attempts: int | None = None,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error: str | None = None
        attempts = 0
        while time.monotonic() < deadline:
            attempts += 1
            try:
                response = _readiness._read_bounded_http_response(
                    health_url,
                    headers=None,
                    maximum_bytes=_MAX_LOCAL_HEALTH_BYTES,
                    deadline=deadline,
                )
                if 200 <= response.status_code < 300:
                    if expected_body is None or response.content == expected_body.encode("utf-8"):
                        return
                    last_error = "HTTP response body did not match the runtime identity"
                else:
                    last_error = f"HTTP {response.status_code}"
            except httpx.HTTPError as exc:
                last_error = str(exc)
            if max_attempts is not None and attempts >= max_attempts:
                break
            _readiness._sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"local service health probe failed: {health_url}: {last_error}")

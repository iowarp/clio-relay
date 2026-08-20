"""Runtime teardown (stop) for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 14, class-mixin
split): the public ``stop`` entry point and its transition-lock-serialized
``_stop_serialized`` implementation (revoke any browser attachment, stop the
desktop/remote connectors, cancel-or-retain the scheduler job, and durably
record the outcome), plus the teardown-policy quartet it exclusively uses:
``_prepare_teardown_intent``, ``_prepare_teardown_policy``,
``_validate_teardown_policy`` (an immutable policy committed before any
cleanup side effect, so a retried stop cannot silently change what it tears
down), and ``_completed_teardown_result`` (idempotent replay of a prior
non-retryable teardown). The two schema constants
(``_GATEWAY_TEARDOWN_POLICY_SCHEMA``, ``_GATEWAY_TEARDOWN_RESULT_SCHEMA``)
move with their only callers.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.queue``, ``self.settings``,
``self.definition``) and calls back into sibling mixins through ``self`` --
browser cleanup (``self._revoke_browser_for_runtime_cleanup``), connector
lifecycle (``self._stop_local_connector``, ``self._stop_allocation_connector``),
reconciliation (``self._reconcile_ownership_intents``,
``self._verified_scheduler_submission``), and scheduler polling
(``self._request_scheduler_provider_cancel``, ``self._wait_for_scheduler_terminal``,
``self._retained_scheduler_resource``). Python's MRO resolves every one of
those through whichever mixin defines it regardless of call origin, so no
cross-mixin qualification is used. The class docstring in
``service_runtime.py`` records the full mixin composition.
"""

from __future__ import annotations

from typing import Literal, cast

from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_results as _results
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_submission_scripts as _submission_scripts
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_remote_scripts import remote_stop_script as _remote_stop_script
from clio_relay.models import GatewaySession, GatewaySessionState, utc_now
from clio_relay.session_wire_models import CleanupResource

_GATEWAY_TEARDOWN_POLICY_SCHEMA = "clio-relay.gateway-teardown-policy.v1"
_GATEWAY_TEARDOWN_RESULT_SCHEMA = "clio-relay.gateway-teardown-result.v1"


class _ServiceRuntimeStopMixin:
    """Tear down a runtime session: connectors, scheduler job, and gateway record."""

    def stop(
        self,
        *,
        session_id: str,
        cancel_scheduler_job: bool = False,
        final_state: GatewaySessionState = GatewaySessionState.CLOSED,
    ) -> _results.ServiceRuntimeStopResult:
        """Serialize and durably replay one owned runtime teardown operation."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        if final_state not in {GatewaySessionState.CLOSED, GatewaySessionState.FAILED}:
            raise ConfigurationError("gateway teardown final state must be closed or failed")
        with self._gateway_transition_lock(session_id):
            return self._stop_serialized(
                session_id=session_id,
                cancel_scheduler_job=cancel_scheduler_job,
                final_state=final_state,
            )

    def _stop_serialized(
        self,
        *,
        session_id: str,
        cancel_scheduler_job: bool,
        final_state: GatewaySessionState,
    ) -> _results.ServiceRuntimeStopResult:
        """Execute teardown while holding the exact cluster/session transition lock."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        session = self._prepare_teardown_intent(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        session = self._prepare_teardown_policy(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
            final_state=final_state,
        )
        replay = self._completed_teardown_result(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
            final_state=final_state,
        )
        if replay is not None:
            return replay
        session = self._reconcile_ownership_intents(session)
        scheduler_contract = _scheduler_contracts._validated_durable_scheduler_contract(
            session, strict=False
        )

        # Reconciliation may refresh durable connector identities, but cannot alter
        # the teardown policy that was committed before any cleanup side effect.
        self._validate_teardown_policy(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
            final_state=final_state,
        )

        session, browser_resource, browser_error = self._revoke_browser_for_runtime_cleanup(session)
        teardown_intent = _primitives._object(session.gateway.get("teardown_intent", {}))
        ownership_intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        transport = _primitives._object(session.gateway.get("transport", {}))
        desktop_connector = _primitives._object(transport.get("desktop_connector", {}))
        remote_connector = _primitives._object(transport.get("remote_connector", {}))
        resources: list[CleanupResource] = []
        errors: list[str] = []
        if browser_resource is not None:
            resources.append(browser_resource)
        if browser_error is not None:
            errors.append(browser_error)
        stopped_local_pid, local_resource = self._stop_local_connector(
            session_id=session.session_id,
            connector=desktop_connector,
            require_record=True,
            absence_verified=_scheduler_contracts._intent_proves_absence(
                ownership_intents,
                "desktop_connector",
            ),
        )
        local_resource = _primitives._bind_cleanup_resource_to_gateway(
            local_resource, session.session_id
        )
        resources.append(local_resource)
        if local_resource.residual:
            errors.append(local_resource.detail or "desktop connector cleanup failed")
        stopped_remote_pid = None
        remote_pid = _primitives._optional_int(remote_connector.get("pid"))
        allocation_scoped = remote_connector.get("execution_scope") == "scheduler_allocation"
        remote_owned = (
            remote_connector.get("owner") == "clio-relay"
            and remote_connector.get("session_id") == session.session_id
        )
        if allocation_scoped:
            try:
                remote_resource = self._stop_allocation_connector(
                    session_id=session.session_id,
                    connector=remote_connector,
                )
            except (ConfigurationError, RelayError) as exc:
                remote_resource = CleanupResource(
                    kind="remote_connector",
                    resource_id=(
                        _primitives._optional_str(remote_connector.get("scheduler_step_id"))
                        or session.session_id
                    ),
                    location=self.definition.ssh_host,
                    provider=_primitives._optional_str(remote_connector.get("scheduler_provider")),
                    action="stop",
                    ownership_verified=False,
                    outcome="refused",
                    residual=True,
                    detail=str(exc),
                )
                errors.append(str(exc))
        elif remote_pid is None:
            absence_verified = _scheduler_contracts._intent_proves_absence(
                ownership_intents,
                "remote_connector",
            )
            remote_resource = CleanupResource(
                kind="remote_connector",
                resource_id=session.session_id,
                location=self.definition.ssh_host,
                action="stop",
                ownership_verified=absence_verified,
                outcome="missing" if absence_verified else "refused",
                verified_after_operation=absence_verified,
                residual=not absence_verified,
                detail=(
                    "no remote connector side effect was proven by its durable intent"
                    if absence_verified
                    else "owned remote connector record is missing or unverified"
                ),
            )
            if remote_resource.residual:
                errors.append(remote_resource.detail or "remote connector record is missing")
        elif not remote_owned:
            remote_resource = CleanupResource(
                kind="remote_connector",
                resource_id=str(remote_pid),
                location=self.definition.ssh_host,
                action="stop",
                ownership_verified=False,
                outcome="refused",
                residual=True,
                detail="connector record does not prove clio-relay session ownership",
            )
            errors.append(remote_resource.detail or "remote connector ownership failed")
        else:
            try:
                remote_output = self._ssh(
                    _remote_stop_script(session_id=session.session_id, pid=remote_pid)
                )
                remote_result = _scheduler_contracts._last_json_object(remote_output)
                remote_outcome = remote_result.get("outcome")
                if not _connector_identity._remote_cleanup_proven(remote_result):
                    raise RelayError(
                        "remote connector cleanup did not prove full process-group absence: "
                        f"{remote_result!r}"
                    )
                if remote_outcome == "stopped":
                    stopped_remote_pid = remote_pid
                remote_resource = CleanupResource(
                    kind="remote_connector",
                    resource_id=str(remote_pid),
                    location=self.definition.ssh_host,
                    action="stop",
                    ownership_verified=True,
                    outcome=cast(Literal["stopped", "missing"], remote_outcome),
                    verified_after_operation=True,
                )
            except RelayError as exc:
                remote_resource = CleanupResource(
                    kind="remote_connector",
                    resource_id=str(remote_pid),
                    location=self.definition.ssh_host,
                    action="stop",
                    ownership_verified=False,
                    outcome="refused",
                    residual=True,
                    detail=str(exc),
                )
                errors.append(str(exc))
        resources.append(
            _primitives._bind_cleanup_resource_to_gateway(remote_resource, session.session_id)
        )
        canceled_scheduler_job = None
        scheduler_intent = _primitives._object(ownership_intents.get("scheduler_submission", {}))
        if scheduler_contract.unresolved_submission:
            unresolved_scheduler = CleanupResource(
                kind="scheduler_job",
                resource_id=str(scheduler_intent.get("submission_id") or session.session_id),
                location=self.definition.ssh_host,
                provider=session.scheduler,
                action="cancel" if cancel_scheduler_job else "retain",
                metadata={"gateway_session_id": session.session_id},
                ownership_verified=False,
                outcome="failed",
                verified_after_operation=False,
                residual=True,
                detail=(
                    "scheduler submission side effect could not be reconciled to an exact job id"
                ),
            )
            resources.append(unresolved_scheduler)
            errors.append(unresolved_scheduler.detail or "scheduler submission is unresolved")
        if session.scheduler_job_id is not None:
            try:
                verified_submission = self._verified_scheduler_submission(
                    session,
                    allow_quiesced_owner_source_recovery=not cancel_scheduler_job,
                )
            except (ConfigurationError, RelayError) as exc:
                scheduler_resource = CleanupResource(
                    kind="scheduler_job",
                    resource_id=session.scheduler_job_id,
                    location=self.definition.ssh_host,
                    provider=session.scheduler,
                    action="cancel" if cancel_scheduler_job else "retain",
                    metadata={"gateway_session_id": session.session_id},
                    ownership_verified=False,
                    outcome="refused",
                    verified_after_operation=False,
                    residual=True,
                    detail=f"scheduler ownership verification failed: {exc}",
                )
            else:
                scheduler_job_id = verified_submission.scheduler_job_id
                spec = verified_submission.spec
                if cancel_scheduler_job:
                    cancel_request_error: str | None = None
                    try:
                        if verified_submission.provider == "external":
                            if spec.cancel_command is None:
                                raise RelayError(
                                    "externally managed runtime has no deployment-driver "
                                    "cancel command"
                                )
                            if spec.status_command is None:
                                raise RelayError(
                                    "externally managed runtime has no deployment-driver "
                                    "status command for terminal cancellation confirmation"
                                )
                            self._ssh(
                                _submission_scripts._template_command_script(
                                    spec.cancel_command, scheduler_job_id
                                )
                            )
                        else:
                            self._request_scheduler_provider_cancel(
                                provider=verified_submission.provider,
                                scheduler_job_id=scheduler_job_id,
                            )
                    except (ConfigurationError, RelayError) as exc:
                        cancel_request_error = str(exc)
                    try:
                        terminal_state = self._wait_for_scheduler_terminal(
                            scheduler=verified_submission.provider,
                            spec=spec,
                            scheduler_job_id=scheduler_job_id,
                        )
                        if terminal_state in _scheduler_contracts._CANCELED_RUNTIME_STATES:
                            canceled_scheduler_job = scheduler_job_id
                            scheduler_resource = CleanupResource(
                                kind="scheduler_job",
                                resource_id=scheduler_job_id,
                                location=self.definition.ssh_host,
                                provider=verified_submission.provider,
                                action="cancel",
                                metadata={"gateway_session_id": session.session_id},
                                ownership_verified=True,
                                outcome="canceled",
                                verified_after_operation=True,
                                observed_state=terminal_state,
                                detail=(
                                    f"canceled scheduler state confirmed: {terminal_state}"
                                    + (
                                        "; the repeated cancel request returned an error: "
                                        f"{cancel_request_error}"
                                        if cancel_request_error is not None
                                        else ""
                                    )
                                ),
                            )
                        else:
                            scheduler_resource = CleanupResource(
                                kind="scheduler_job",
                                resource_id=scheduler_job_id,
                                location=self.definition.ssh_host,
                                provider=verified_submission.provider,
                                action="cancel",
                                metadata={"gateway_session_id": session.session_id},
                                ownership_verified=True,
                                outcome="terminal",
                                verified_after_operation=True,
                                observed_state=terminal_state,
                                detail=(
                                    "cancel was requested, but the observed terminal scheduler "
                                    f"state was {terminal_state}; cancellation is not claimed"
                                    + (
                                        "; the repeated cancel request returned an error: "
                                        f"{cancel_request_error}"
                                        if cancel_request_error is not None
                                        else ""
                                    )
                                ),
                            )
                    except (ConfigurationError, RelayError) as exc:
                        detail = str(exc)
                        if cancel_request_error is not None:
                            detail = (
                                f"scheduler cancel request failed: {cancel_request_error}; "
                                f"terminal-state verification failed: {detail}"
                            )
                        scheduler_resource = CleanupResource(
                            kind="scheduler_job",
                            resource_id=scheduler_job_id,
                            location=self.definition.ssh_host,
                            provider=verified_submission.provider,
                            action="cancel",
                            metadata={"gateway_session_id": session.session_id},
                            ownership_verified=True,
                            outcome="failed",
                            residual=True,
                            detail=detail,
                        )
                        errors.append(detail)
                else:
                    scheduler_resource = self._retained_scheduler_resource(
                        session=session,
                        spec=spec,
                    )
            resources.append(scheduler_resource)
            if scheduler_resource.residual:
                errors.append(
                    scheduler_resource.detail or "scheduler lifecycle verification failed"
                )
        cleanup_operation_id = _scheduler_contracts._required_intent_str(
            teardown_intent, "operation_id"
        )
        resources = [
            resource.model_copy(
                update={
                    "metadata": {
                        **resource.metadata,
                        "cleanup_operation_id": cleanup_operation_id,
                        "cancel_scheduler_job": cancel_scheduler_job,
                    }
                }
            )
            for resource in resources
        ]
        cleanup_succeeded = not errors and not any(resource.residual for resource in resources)
        effective_state = (
            final_state
            if cleanup_succeeded
            else (
                GatewaySessionState.FAILED
                if final_state == GatewaySessionState.FAILED
                else GatewaySessionState.DEGRADED
            )
        )
        gateway_resource = CleanupResource(
            kind="gateway_record",
            resource_id=session_id,
            location=str(self.settings.core_dir),
            action="close",
            ownership_verified=True,
            outcome="closed" if cleanup_succeeded else "failed",
            verified_after_operation=cleanup_succeeded,
            residual=not cleanup_succeeded,
            detail=None if cleanup_succeeded else "gateway remains retryable after cleanup failure",
            metadata={
                "cleanup_operation_id": cleanup_operation_id,
                "cancel_scheduler_job": cancel_scheduler_job,
                "gateway_session_id": session_id,
            },
        )
        resources.append(gateway_resource)
        cleanup_completed_at = utc_now().isoformat()
        updated = self.queue.update_gateway_session(
            session_id,
            state=effective_state,
            expected_updated_at=session.updated_at,
            allow_owned_runtime_close=effective_state == GatewaySessionState.CLOSED,
            metadata={
                "cleanup_at": cleanup_completed_at,
                "closed_at": (
                    cleanup_completed_at if effective_state == GatewaySessionState.CLOSED else None
                ),
                "cancel_scheduler_job": cancel_scheduler_job,
                "cleanup_retryable": not cleanup_succeeded,
                "cleanup_errors": errors,
                "cleanup_operation_id": cleanup_operation_id,
            },
            gateway={
                **session.gateway,
                "teardown": {
                    "schema_version": _GATEWAY_TEARDOWN_RESULT_SCHEMA,
                    "operation_id": cleanup_operation_id,
                    "gateway_session_id": session_id,
                    "mode": "teardown",
                    "cancel_scheduler_job": cancel_scheduler_job,
                    "requested_final_state": final_state.value,
                    "effective_state": effective_state.value,
                    "completed_at": cleanup_completed_at,
                    "retryable": not cleanup_succeeded,
                    "stopped_local_pid": stopped_local_pid,
                    "stopped_remote_pid": stopped_remote_pid,
                    "canceled_scheduler_job": canceled_scheduler_job,
                    "resources": [resource.model_dump(mode="json") for resource in resources],
                    "errors": errors,
                },
            },
        )
        return _results.ServiceRuntimeStopResult(
            session=updated,
            mode="teardown",
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=stopped_remote_pid,
            canceled_scheduler_job=canceled_scheduler_job,
            resources=resources,
            errors=errors,
        )

    def _prepare_teardown_intent(
        self,
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
    ) -> GatewaySession:
        """Persist an immutable cleanup policy before any teardown side effect."""
        return self.queue.prepare_gateway_teardown_intent(
            session.session_id,
            cancel_scheduler_job=cancel_scheduler_job,
        )

    def _prepare_teardown_policy(
        self,
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
        final_state: GatewaySessionState,
    ) -> GatewaySession:
        """Persist or validate immutable cleanup policy before cleanup side effects."""
        intent = _scheduler_contracts._validated_gateway_teardown_intent(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        raw_policy = session.gateway.get("teardown_policy")
        if raw_policy is not None:
            self._validate_teardown_policy(
                session,
                cancel_scheduler_job=cancel_scheduler_job,
                final_state=final_state,
            )
            return session
        if session.state is GatewaySessionState.CLOSED or (
            session.metadata.get("cleanup_retryable") is False
            and session.gateway.get("teardown") is not None
        ):
            raise RelayError("completed gateway teardown evidence is invalid")
        policy: dict[str, object] = {
            "schema_version": _GATEWAY_TEARDOWN_POLICY_SCHEMA,
            "operation_id": intent["operation_id"],
            "gateway_session_id": session.session_id,
            "cancel_scheduler_job": cancel_scheduler_job,
            "final_state": final_state.value,
            "committed_at": utc_now().isoformat(),
        }
        return self.queue.update_gateway_session(
            session.session_id,
            expected_updated_at=session.updated_at,
            metadata={
                "cleanup_at": None,
                "closed_at": None,
                "cancel_scheduler_job": cancel_scheduler_job,
                "cleanup_retryable": True,
                "cleanup_errors": [],
                "cleanup_operation_id": intent["operation_id"],
            },
            gateway={**session.gateway, "teardown_policy": policy},
        )

    def _validate_teardown_policy(
        self,
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
        final_state: GatewaySessionState,
    ) -> dict[str, object]:
        """Validate the exact immutable cleanup policy committed for this operation."""
        intent = _scheduler_contracts._validated_gateway_teardown_intent(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        raw_policy = session.gateway.get("teardown_policy")
        if not isinstance(raw_policy, dict):
            raise RelayError("gateway teardown policy is invalid")
        policy = cast(dict[str, object], raw_policy)
        if set(policy) != {
            "schema_version",
            "operation_id",
            "gateway_session_id",
            "cancel_scheduler_job",
            "final_state",
            "committed_at",
        }:
            raise RelayError("gateway teardown policy is invalid")
        committed_at = policy.get("committed_at")
        if (
            policy.get("schema_version") != _GATEWAY_TEARDOWN_POLICY_SCHEMA
            or policy.get("operation_id") != intent["operation_id"]
            or policy.get("gateway_session_id") != session.session_id
            or not isinstance(committed_at, str)
        ):
            raise RelayError("gateway teardown policy is invalid")
        _scheduler_contracts._gateway_teardown_timestamp(committed_at)
        if policy.get("cancel_scheduler_job") is not cancel_scheduler_job:
            raise RelayError(
                "gateway cleanup policy changed during retry; resume with the original "
                f"cancel_scheduler_job={policy.get('cancel_scheduler_job')} policy"
            )
        if policy.get("final_state") != final_state.value:
            raise RelayError(
                "gateway cleanup final-state policy changed during retry; resume with the "
                f"original final_state={policy.get('final_state')} policy"
            )
        return policy

    def _completed_teardown_result(
        self,
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
        final_state: GatewaySessionState,
    ) -> _results.ServiceRuntimeStopResult | None:
        """Rehydrate exact non-retryable teardown evidence without repeating side effects."""
        raw_result = session.gateway.get("teardown")
        retryable = session.metadata.get("cleanup_retryable")
        typed_result = cast(dict[str, object], raw_result) if isinstance(raw_result, dict) else None
        result_marks_completed = bool(
            typed_result is not None
            and typed_result.get("schema_version") == _GATEWAY_TEARDOWN_RESULT_SCHEMA
            and typed_result.get("retryable") is False
        )
        if retryable is True:
            if result_marks_completed or session.state is GatewaySessionState.CLOSED:
                raise RelayError("completed gateway teardown evidence is invalid")
            return None
        if retryable is not False:
            if result_marks_completed or session.state is GatewaySessionState.CLOSED:
                raise RelayError("completed gateway teardown evidence is invalid")
            return None
        policy = self._validate_teardown_policy(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
            final_state=final_state,
        )
        if typed_result is None:
            raise RelayError("completed gateway teardown evidence is invalid")
        result = typed_result
        expected_fields = {
            "schema_version",
            "operation_id",
            "gateway_session_id",
            "mode",
            "cancel_scheduler_job",
            "requested_final_state",
            "effective_state",
            "completed_at",
            "retryable",
            "stopped_local_pid",
            "stopped_remote_pid",
            "canceled_scheduler_job",
            "resources",
            "errors",
        }
        if set(result) != expected_fields:
            raise RelayError("completed gateway teardown evidence is invalid")
        operation_id = cast(str, policy["operation_id"])
        completed_at = result.get("completed_at")
        if (
            result.get("schema_version") != _GATEWAY_TEARDOWN_RESULT_SCHEMA
            or result.get("operation_id") != operation_id
            or result.get("gateway_session_id") != session.session_id
            or result.get("mode") != "teardown"
            or result.get("cancel_scheduler_job") is not cancel_scheduler_job
            or result.get("requested_final_state") != final_state.value
            or result.get("effective_state") != final_state.value
            or result.get("retryable") is not False
            or not isinstance(completed_at, str)
            or session.state.value != result.get("effective_state")
        ):
            raise RelayError("completed gateway teardown evidence is invalid")
        _scheduler_contracts._gateway_teardown_timestamp(completed_at)
        stopped_local_pid = _scheduler_contracts._strict_optional_positive_int(
            result.get("stopped_local_pid")
        )
        stopped_remote_pid = _scheduler_contracts._strict_optional_positive_int(
            result.get("stopped_remote_pid")
        )
        canceled_scheduler_job = _scheduler_contracts._strict_optional_nonempty_str(
            result.get("canceled_scheduler_job")
        )
        resources, errors = _scheduler_contracts._validated_completed_resource_lists(
            result,
            error="completed gateway teardown evidence is invalid",
        )
        if errors or any(resource.residual for resource in resources):
            raise RelayError("completed gateway teardown evidence is invalid")
        _scheduler_contracts._validate_completed_teardown_resources(
            session,
            resources=resources,
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=stopped_remote_pid,
            canceled_scheduler_job=canceled_scheduler_job,
            operation_id=operation_id,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        if not _scheduler_contracts._completed_teardown_metadata_matches(
            session,
            operation_id=operation_id,
            cancel_scheduler_job=cancel_scheduler_job,
            completed_at=completed_at,
            final_state=final_state,
            errors=errors,
        ):
            raise RelayError("completed gateway teardown evidence is invalid")
        return _results.ServiceRuntimeStopResult(
            session=session,
            mode="teardown",
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=stopped_remote_pid,
            canceled_scheduler_job=canceled_scheduler_job,
            resources=resources,
            errors=errors,
        )

"""Desktop-connector-only detach for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 15, class-mixin
split): the public ``detach`` entry point and its transition-lock-serialized
``_detach_serialized`` implementation (stop only the desktop connector,
retain the remote connector and scheduler job for a later reattach), the
matching ``_prepare_detach_intent``/``_completed_detach_result`` durable
replay pair, ``_consume_completed_detach_for_attach`` (retires a validated
detach generation before attach creates its replacement connector), and the
resumability predicates that decide whether a detached or pre-ready
submission can safely advance again
(``_pending_submission_has_no_connector_side_effects``,
``_detached_pending_submission_can_resume``,
``_pre_ready_submission_can_resume``). These predicates are interleaved
with the detach-intent helpers in the original source because they are one
concern: what a detached generation proves and who may resume it. The
``_GATEWAY_DETACH_RESULT_SCHEMA`` constant moves with its only callers.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.queue``, ``self.settings``,
``self.definition``) and calls back into sibling mixins through ``self`` --
browser cleanup (``self._revoke_browser_for_runtime_cleanup``), connector
lifecycle (``self._stop_local_connector``,
``self._retained_allocation_connector_resource``), reconciliation
(``self._reconcile_ownership_intents``, ``self._verified_scheduler_submission``,
``self._retained_scheduler_resource``), and the start-cluster reconciliation
predicate ``self._scheduler_submission_reconciliation_is_pending``. The
attach mixin calls back into this module's resumability predicates the same
way. Python's MRO resolves every one of those through whichever mixin
defines it regardless of call origin, so no cross-mixin qualification is
used. The class docstring in ``service_runtime.py`` records the full mixin
composition.
"""

from __future__ import annotations

import secrets
from typing import cast

from clio_relay import service_runtime_connector_step_scripts as _connector_step_scripts
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_results as _results
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import GatewaySession, GatewaySessionState, utc_now
from clio_relay.session_wire_models import CleanupResource

_GATEWAY_DETACH_RESULT_SCHEMA = "clio-relay.gateway-detach-result.v1"


class _ServiceRuntimeDetachMixin:
    """Detach the desktop connector while retaining the remote side for reattach."""

    def detach(self, *, session_id: str) -> _results.ServiceRuntimeStopResult:
        """Serialize detachment against attach and teardown for this gateway."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        with self._gateway_transition_lock(session_id):
            return self._detach_serialized(session_id=session_id)

    def _detach_serialized(self, *, session_id: str) -> _results.ServiceRuntimeStopResult:
        """Stop only the desktop connector while holding the gateway transition lock."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        if session.state is GatewaySessionState.CLOSED:
            raise ConfigurationError(f"gateway session {session_id} is closed")
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot detach"
            )
        session = self._prepare_detach_intent(session)
        replay = self._completed_detach_result(session)
        if replay is not None:
            return replay
        session = self._reconcile_ownership_intents(session)
        pending_without_connectors = self._pending_submission_has_no_connector_side_effects(session)
        scheduler_contract = _scheduler_contracts._validated_durable_scheduler_contract(
            session, strict=False
        )
        session, browser_resource, browser_error = self._revoke_browser_for_runtime_cleanup(session)
        transport = _primitives._object(session.gateway.get("transport", {}))
        desktop_connector = _primitives._object(transport.get("desktop_connector", {}))
        ownership_intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        desktop_absence_verified = _scheduler_contracts._intent_proves_absence(
            ownership_intents,
            "desktop_connector",
        )
        stopped_local_pid, local_resource = self._stop_local_connector(
            session_id=session.session_id,
            connector=desktop_connector,
            require_record=not (pending_without_connectors or desktop_absence_verified),
            absence_verified=pending_without_connectors or desktop_absence_verified,
        )
        local_resource = _primitives._bind_cleanup_resource_to_gateway(
            local_resource, session.session_id
        )
        resources = [local_resource]
        if browser_resource is not None:
            resources.insert(0, browser_resource)
        errors = (
            [local_resource.detail] if local_resource.residual and local_resource.detail else []
        )
        if browser_error is not None:
            errors.append(browser_error)
        remote_connector = _primitives._object(transport.get("remote_connector", {}))
        remote_pid = _primitives._optional_int(remote_connector.get("pid"))
        if remote_connector.get("execution_scope") == "scheduler_allocation":
            try:
                allocation_resource = self._retained_allocation_connector_resource(
                    session_id=session.session_id,
                    connector=remote_connector,
                )
            except (ConfigurationError, RelayError) as exc:
                allocation_resource = CleanupResource(
                    kind="remote_connector",
                    resource_id=(
                        _primitives._optional_str(remote_connector.get("scheduler_step_id"))
                        or session.session_id
                    ),
                    location=self.definition.ssh_host,
                    provider=_primitives._optional_str(remote_connector.get("scheduler_provider")),
                    action="retain",
                    ownership_verified=False,
                    outcome="failed",
                    residual=True,
                    detail=str(exc),
                )
            resources.append(
                _primitives._bind_cleanup_resource_to_gateway(
                    allocation_resource,
                    session.session_id,
                )
            )
            if allocation_resource.residual:
                errors.append(
                    allocation_resource.detail
                    or "allocation connector retention could not be proven"
                )
        elif remote_pid is not None:
            remote_owned = (
                remote_connector.get("owner") == "clio-relay"
                and remote_connector.get("session_id") == session.session_id
            )
            remote_verified = False
            remote_detail = "remote connector ownership record is incomplete"
            if remote_owned:
                try:
                    remote_status = _scheduler_contracts._last_json_object(
                        self._ssh(
                            _connector_step_scripts._remote_connector_status_script(
                                session_id=session.session_id,
                                pid=remote_pid,
                            )
                        )
                    )
                    remote_verified = (
                        remote_status.get("ownership_verified") is True
                        and remote_status.get("running") is True
                        and isinstance(remote_status.get("matching_pids"), list)
                        and bool(remote_status["matching_pids"])
                    )
                    remote_detail = (
                        "remote connector intentionally retained for reattachment"
                        if remote_verified
                        else "remote connector retention could not be proven live"
                    )
                except RelayError as exc:
                    remote_detail = str(exc)
            resources.append(
                _primitives._bind_cleanup_resource_to_gateway(
                    CleanupResource(
                        kind="remote_connector",
                        resource_id=str(remote_pid),
                        location=self.definition.ssh_host,
                        action="retain",
                        ownership_verified=remote_verified,
                        outcome="retained" if remote_verified else "failed",
                        verified_after_operation=remote_verified,
                        residual=not remote_verified,
                        detail=remote_detail,
                    ),
                    session.session_id,
                )
            )
            if not remote_verified:
                errors.append(remote_detail)
        elif pending_without_connectors:
            resources.append(
                _primitives._bind_cleanup_resource_to_gateway(
                    CleanupResource(
                        kind="remote_connector",
                        resource_id=session.session_id,
                        location=self.definition.ssh_host,
                        action="retain",
                        ownership_verified=True,
                        outcome="missing",
                        verified_after_operation=True,
                        observed_state="not_created",
                        detail=(
                            "durable connector intent proves no remote connector side effect "
                            "was created"
                        ),
                    ),
                    session.session_id,
                )
            )
        else:
            errors.append("owned remote connector record is missing during detach")
            resources.append(
                _primitives._bind_cleanup_resource_to_gateway(
                    CleanupResource(
                        kind="remote_connector",
                        resource_id=session.session_id,
                        location=self.definition.ssh_host,
                        action="retain",
                        ownership_verified=False,
                        outcome="failed",
                        residual=True,
                        detail="owned remote connector record is missing during detach",
                    ),
                    session.session_id,
                )
            )
        if scheduler_contract.unresolved_submission:
            scheduler_intent = _primitives._object(
                _primitives._object(session.gateway.get("ownership_intents", {})).get(
                    "scheduler_submission",
                    {},
                )
            )
            submission_id = _scheduler_contracts._required_intent_str(
                scheduler_intent, "submission_id"
            )
            submission_marker = _scheduler_contracts._required_intent_str(
                scheduler_intent,
                "submission_marker",
            )
            scheduler_resource = CleanupResource(
                kind="scheduler_submission",
                resource_id=submission_id,
                location=self.definition.ssh_host,
                provider=scheduler_contract.provider,
                action="retain",
                metadata={
                    "gateway_session_id": session.session_id,
                    "submission_id": submission_id,
                    "submission_marker": submission_marker,
                    "scheduler_job_id": None,
                    "submission_outcome": "unresolved",
                    "cancel_requested": False,
                    "resubmit_requested": False,
                },
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                observed_state="intent_recorded",
                residual=False,
                detail=(
                    "exact scheduler submission intent retained without claiming a scheduler job"
                ),
            )
            resources.append(scheduler_resource)
        elif session.scheduler_job_id is not None:
            try:
                verified_submission = self._verified_scheduler_submission(session)
            except (ConfigurationError, RelayError) as exc:
                scheduler_resource = CleanupResource(
                    kind="scheduler_job",
                    resource_id=session.scheduler_job_id,
                    location=self.definition.ssh_host,
                    provider=session.scheduler,
                    action="retain",
                    metadata={"gateway_session_id": session.session_id},
                    ownership_verified=False,
                    outcome="refused",
                    verified_after_operation=False,
                    residual=True,
                    detail=f"scheduler ownership verification failed: {exc}",
                )
            else:
                scheduler_resource = self._retained_scheduler_resource(
                    session=session,
                    spec=verified_submission.spec,
                )
            resources.append(scheduler_resource)
            if scheduler_resource.residual:
                errors.append(
                    scheduler_resource.detail or "scheduler retention verification failed"
                )
            elif scheduler_resource.outcome in {"terminal", "missing"}:
                errors.append(
                    f"scheduler job is {scheduler_resource.outcome}; detached runtime cannot "
                    "be proven reattachable"
                )
        resources.append(
            CleanupResource(
                kind="gateway_record",
                resource_id=session.session_id,
                location=str(self.settings.core_dir),
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                observed_state=GatewaySessionState.DEGRADED.value,
                detail="gateway record retained for an explicit later reattachment or teardown",
                metadata={"gateway_session_id": session.session_id},
            )
        )
        detach_intent = _scheduler_contracts._validated_gateway_detach_intent(session)
        detach_operation_id = cast(str, detach_intent["operation_id"])
        resources = [
            resource.model_copy(
                update={
                    "metadata": {
                        **resource.metadata,
                        "cleanup_operation_id": detach_operation_id,
                        "cancel_scheduler_job": False,
                    }
                }
            )
            for resource in resources
        ]
        detach_retryable = any(item.residual for item in resources)
        detached_at = utc_now().isoformat()
        updated = self.queue.update_gateway_session(
            session_id,
            state=GatewaySessionState.DEGRADED,
            expected_updated_at=session.updated_at,
            metadata={
                "detached_at": detached_at,
                "cleanup_retryable": detach_retryable,
                "cleanup_errors": errors,
                "detach_operation_id": detach_operation_id,
                "detach_retryable": detach_retryable,
                "detach_errors": errors,
            },
            gateway={
                **session.gateway,
                "detach": {
                    "schema_version": _GATEWAY_DETACH_RESULT_SCHEMA,
                    "operation_id": detach_operation_id,
                    "gateway_session_id": session_id,
                    "mode": "detach",
                    "completed_at": detached_at,
                    "retryable": detach_retryable,
                    "stopped_local_pid": stopped_local_pid,
                    "resources": [resource.model_dump(mode="json") for resource in resources],
                    "errors": errors,
                },
            },
        )
        return _results.ServiceRuntimeStopResult(
            session=updated,
            mode="detach",
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=None,
            canceled_scheduler_job=None,
            resources=resources,
            errors=errors,
        )

    def _prepare_detach_intent(self, session: GatewaySession) -> GatewaySession:
        """Persist or validate one detach operation before destructive cleanup."""
        raw_intent = session.gateway.get("detach_intent")
        if raw_intent is not None:
            _scheduler_contracts._validated_gateway_detach_intent(session)
            return session
        raw_result = session.gateway.get("detach")
        versioned_result = (
            cast(dict[str, object], raw_result).get("schema_version")
            == _GATEWAY_DETACH_RESULT_SCHEMA
            if isinstance(raw_result, dict)
            else False
        )
        if versioned_result or session.metadata.get("detach_operation_id") is not None:
            raise RelayError("gateway detach evidence is invalid")
        operation_id = f"gateway_detach_{secrets.token_hex(16)}"
        created_at = utc_now().isoformat()
        gateway = dict(session.gateway)
        # A legacy, unversioned detach observation cannot be replayed as durable
        # evidence. A new operation supersedes it and proves the current state.
        gateway.pop("detach", None)
        gateway["detach_intent"] = {
            "schema_version": _scheduler_contracts._GATEWAY_DETACH_INTENT_SCHEMA,
            "operation_id": operation_id,
            "gateway_session_id": session.session_id,
            "created_at": created_at,
        }
        return self.queue.update_gateway_session(
            session.session_id,
            expected_updated_at=session.updated_at,
            metadata={
                "detach_operation_id": operation_id,
                "detach_retryable": True,
                "detach_errors": [],
            },
            gateway=gateway,
        )

    def _completed_detach_result(
        self,
        session: GatewaySession,
    ) -> _results.ServiceRuntimeStopResult | None:
        """Rehydrate exact completed detach evidence without repeating side effects."""
        intent = _scheduler_contracts._validated_gateway_detach_intent(session)
        raw_result = session.gateway.get("detach")
        retryable = session.metadata.get("detach_retryable")
        result = cast(dict[str, object], raw_result) if isinstance(raw_result, dict) else None
        result_marks_completed = bool(
            result is not None
            and result.get("schema_version") == _GATEWAY_DETACH_RESULT_SCHEMA
            and result.get("retryable") is False
        )
        if retryable is True:
            if result_marks_completed:
                raise RelayError("gateway detach evidence is invalid")
            return None
        if retryable is not False:
            if result_marks_completed:
                raise RelayError("gateway detach evidence is invalid")
            return None
        if result is None or set(result) != {
            "schema_version",
            "operation_id",
            "gateway_session_id",
            "mode",
            "completed_at",
            "retryable",
            "stopped_local_pid",
            "resources",
            "errors",
        }:
            raise RelayError("gateway detach evidence is invalid")
        completed_at = result.get("completed_at")
        operation_id = cast(str, intent["operation_id"])
        if (
            result.get("schema_version") != _GATEWAY_DETACH_RESULT_SCHEMA
            or result.get("operation_id") != operation_id
            or result.get("gateway_session_id") != session.session_id
            or result.get("mode") != "detach"
            or result.get("retryable") is not False
            or not isinstance(completed_at, str)
            or session.state is not GatewaySessionState.DEGRADED
        ):
            raise RelayError("gateway detach evidence is invalid")
        _scheduler_contracts._gateway_teardown_timestamp(completed_at)
        stopped_local_pid = _scheduler_contracts._strict_optional_positive_int(
            result.get("stopped_local_pid")
        )
        resources, errors = _scheduler_contracts._validated_completed_resource_lists(
            result,
            error="gateway detach evidence is invalid",
        )
        _scheduler_contracts._validate_completed_detach_resources(
            session,
            resources=resources,
            stopped_local_pid=stopped_local_pid,
            operation_id=operation_id,
        )
        if not _scheduler_contracts._completed_detach_metadata_matches(
            session,
            operation_id=operation_id,
            completed_at=completed_at,
            errors=errors,
        ):
            raise RelayError("gateway detach evidence is invalid")
        return _results.ServiceRuntimeStopResult(
            session=session,
            mode="detach",
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=None,
            canceled_scheduler_job=None,
            resources=resources,
            errors=errors,
        )

    def _consume_completed_detach_for_attach(self, session: GatewaySession) -> GatewaySession:
        """Retire one validated detach generation before creating its replacement connector."""
        gateway = dict(session.gateway)
        gateway.pop("detach", None)
        gateway.pop("detach_intent", None)
        return self.queue.update_gateway_session(
            session.session_id,
            expected_updated_at=session.updated_at,
            metadata={
                "detached_at": None,
                "detach_operation_id": None,
                "detach_retryable": None,
                "detach_errors": [],
            },
            gateway=gateway,
        )

    def _pending_submission_has_no_connector_side_effects(
        self,
        session: GatewaySession,
    ) -> bool:
        """Prove a not-yet-ready submission has never launched either connector."""

        if session.state not in {
            GatewaySessionState.SUBMITTED,
            GatewaySessionState.PENDING,
            GatewaySessionState.ALLOCATED,
            GatewaySessionState.STARTING,
            GatewaySessionState.DEGRADED,
        }:
            return False
        if "service" in session.gateway:
            return False
        transport = _primitives._object(session.gateway.get("transport", {}))
        if _primitives._object(transport.get("remote_connector", {})) or _primitives._object(
            transport.get("desktop_connector", {})
        ):
            return False
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        scheduler_intent = _primitives._object(intents.get("scheduler_submission", {}))
        scheduler_identity_exact = (
            self._scheduler_submission_reconciliation_is_pending(session)
            if session.scheduler_job_id is None
            else scheduler_intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and scheduler_intent.get("state") == "recorded"
            and scheduler_intent.get("scheduler_provider") == session.scheduler
            and scheduler_intent.get("scheduler_job_id") == session.scheduler_job_id
        )
        return scheduler_identity_exact and all(
            _primitives._object(intents.get(role, {})).get("schema_version")
            == _primitives._OWNERSHIP_INTENT_SCHEMA
            and _primitives._object(intents.get(role, {})).get("state")
            in {"not_started", "absent_verified"}
            for role in ("remote_connector", "desktop_connector")
        )

    def _detached_pending_submission_can_resume(self, session: GatewaySession) -> bool:
        """Return whether a detached pre-ready submission can safely advance again."""

        if (
            session.state is not GatewaySessionState.DEGRADED
            or session.gateway.get("detach_intent") is None
            or "service" in session.gateway
        ):
            return False
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        scheduler_intent = _primitives._object(intents.get("scheduler_submission", {}))
        return bool(
            self._scheduler_submission_reconciliation_is_pending(session)
            if session.scheduler_job_id is None
            else scheduler_intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and scheduler_intent.get("state") == "recorded"
            and scheduler_intent.get("scheduler_job_id") == session.scheduler_job_id
        )

    def _pre_ready_submission_can_resume(self, session: GatewaySession) -> bool:
        """Return whether attach should advance an existing pre-ready start in place."""
        return bool(
            session.state
            in {
                GatewaySessionState.SUBMITTED,
                GatewaySessionState.PENDING,
                GatewaySessionState.ALLOCATED,
                GatewaySessionState.STARTING,
            }
            and session.gateway.get("teardown_intent") is None
            and "service" not in session.gateway
        )

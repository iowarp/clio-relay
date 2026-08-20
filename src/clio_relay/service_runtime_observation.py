"""Scheduler/runtime observation, verification, and local-connector stop for
``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 18, class-mixin
split): ``_verified_scheduler_submission`` (prove the exact scheduler
provider and job ID from either a verified JARVIS binding or the
relay-created remote sidecar, with a narrow quiesced-owner direct-source
fallback gated by ``_quiesced_owner_source_recovery_is_authorized``),
``_stop_local_connector`` (the shared desktop-connector termination used by
start rollback, stop, detach, and attach rollback) and its
``_remove_unpublished_local_connector_files`` cleanup, the single-shot
observation core ``_observe_allocation_and_health_once`` (scheduler status,
then runtime status, then one health probe -- never blocks, always returns
either a live node or records a pending observation) and its
``_record_runtime_observation_pending`` persister, ``_retained_scheduler_resource``
(prove a scheduler job's state for a stop/detach that intentionally retains
it), and the scheduler-polling primitives
``_observe_runtime_state``/``_observe_scheduler_state``/``_wait_for_scheduler_terminal``/
``_poll_scheduler_provider``/``_request_scheduler_provider_cancel``.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.queue``, ``self.settings``,
``self.definition``, ``self.cluster``, ``self.sleep``) and calls back into
sibling mixins through ``self`` -- durable-session update (``self._update``,
``self._ssh``). Python's MRO resolves every one of those through whichever
mixin defines it regardless of call origin, so no cross-mixin qualification
is used. The class docstring in ``service_runtime.py`` records the full
mixin composition.
"""

from __future__ import annotations

import json
import time

from clio_relay import service_runtime_connector_step_scripts as _connector_step_scripts
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_submission_scripts as _submission_scripts
from clio_relay import service_runtime_types as _types
from clio_relay.errors import ConfigurationError, QueueConflictError, RelayError
from clio_relay.jarvis_service_runtime import reverify_jarvis_service_runtime
from clio_relay.models import (
    GatewaySession,
    GatewaySessionState,
    SchedulerPhase,
    SchedulerStatus,
    ServiceRuntimeSpec,
    utc_now,
)
from clio_relay.owner_session_admission import desktop_owner_session_admission_id
from clio_relay.scheduler_providers import provider_for_scheduler
from clio_relay.session_wire_models import CleanupResource


class _ServiceRuntimeObservationMixin:
    """Observe scheduler/runtime state and stop the desktop connector."""

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

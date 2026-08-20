"""Ownership-intent reconciliation for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 17, class-mixin
split): ``_reconcile_ownership_intents`` -- the crash-recovery core that
recovers scheduler submission and connector identities written before a
hard exit, by consulting each durable intent's SSH-observed sidecar rather
than trusting in-memory state -- its ``_reconcile_allocation_connector_intent``
helper (recover or disprove a scheduler-allocation connector by its
provider marker), and the identity-binding validators it calls,
``_validate_remote_connector_intent_binding`` /
``_validate_local_connector_intent_binding`` (a connector record must prove
every field a durable intent recorded before either is trusted), plus
``_connector_records_match`` and ``_local_connector_intent`` (the desktop
connector's rediscovery identity).

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.settings``, ``self.definition``)
and calls back into sibling mixins through ``self`` -- the SSH transport
(``self._ssh``), durable-session update (``self._update``), local connector
discovery (``self._connector_identity`` module, not a mixin call), and the
allocation connector step poll (``self._poll_allocation_connector_step``,
``self._allocation_connector_identity``, owned by the remote-connector
lifecycle mixin). Python's MRO resolves every one of those through
whichever mixin defines it regardless of call origin, so no cross-mixin
qualification is used. The class docstring in ``service_runtime.py``
records the full mixin composition.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, cast

from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_connector_step_scripts as _connector_step_scripts
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_submission_scripts as _submission_scripts
from clio_relay import service_runtime_types as _types
from clio_relay.errors import RelayError
from clio_relay.models import (
    GatewaySession,
    GatewaySessionState,
    SchedulerConnectorPlacement,
    SchedulerConnectorStepIdentity,
    utc_now,
)


class _ServiceRuntimeReconciliationMixin:
    """Recover durable scheduler/connector identities after a hard exit."""

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

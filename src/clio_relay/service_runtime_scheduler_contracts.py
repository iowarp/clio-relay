"""Scheduler-submission parsing plus gateway intent/resource-evidence validation.

Extracted from ``service_runtime.py`` (#231 rework slice). Two sibling
concerns share this module because they are mutually coupled, not
independently splittable: completed-resource validation calls the intent
validators for absence proofs and the durable scheduler contract, and the
intent validators call back into the shared teardown timestamp parser.
Covers deployment-driver output parsing (``RuntimeSubmission``/
``RuntimeStatus``), durable ownership-intent construction/validation
(``_new_ownership_intent``, ``_validated_durable_scheduler_contract``, the
teardown/detach intent validators, the exact-sidecar reconciliation-failure
evidence builder), and completed lifecycle-resource evidence validation
(``_validate_completed_detach_resources``,
``_validate_completed_teardown_resources``, and their timestamp/metadata checkers).

Depends only on ``service_runtime_primitives`` and ``service_runtime_types``
-- never on the supervisor class, which imports these names back qualified
through this module instead.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal, cast

from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_types as _types
from clio_relay.errors import RelayError
from clio_relay.jarvis_service_runtime import JarvisServiceRuntimeBinding
from clio_relay.models import GatewaySession, GatewaySessionState, ServiceRuntimeSpec, utc_now
from clio_relay.session_wire_models import CleanupResource

_GATEWAY_DETACH_INTENT_SCHEMA = "clio-relay.gateway-detach-intent.v1"
_TERMINAL_RUNTIME_STATES = {
    "canceled",
    "cancelled",
    "completed",
    "failed",
    "terminated",
    "timeout",
}
_ACTIVE_RUNTIME_STATES = {
    "submitted",
    "pending",
    "queued",
    "allocated",
    "starting",
    "ready",
    "running",
}
_CANCELED_RUNTIME_STATES = {"canceled", "cancelled"}


@dataclass(frozen=True)
class RuntimeSubmission:
    """Structured submission result emitted by a deployment driver."""

    scheduler_job_id: str
    service_host: str | None = None


@dataclass(frozen=True)
class RuntimeStatus:
    """Structured status emitted by a deployment driver."""

    state: str | None = None
    service_host: str | None = None
    reason: str | None = None
    events: list[dict[str, object]] | None = None


def _parse_runtime_submission(output: str) -> RuntimeSubmission:
    """Parse structured JSON submission output from a deployment driver."""
    record = _last_json_object(output)
    scheduler_job_id = record.get("scheduler_job_id")
    if not isinstance(scheduler_job_id, str) or scheduler_job_id == "":
        raise RelayError(
            f"deployment output must include JSON field scheduler_job_id; received: {output!r}"
        )
    service_host = record.get("service_host")
    if service_host is not None and not isinstance(service_host, str):
        raise RelayError("deployment output JSON field service_host must be a string")
    return RuntimeSubmission(scheduler_job_id=scheduler_job_id, service_host=service_host)


def _parse_runtime_status(output: str) -> RuntimeStatus:
    """Parse structured JSON status output from a deployment driver."""
    record = _last_json_object(output)
    state = record.get("state")
    service_host = record.get("service_host")
    reason = record.get("reason")
    events = _runtime_events(record.get("events"))
    return RuntimeStatus(
        state=state if isinstance(state, str) else None,
        service_host=service_host if isinstance(service_host, str) else None,
        reason=reason if isinstance(reason, str) else None,
        events=events,
    )


def _last_json_object(output: str) -> dict[str, object]:
    stripped_output = output.strip()
    if stripped_output:
        try:
            loaded_output = json.loads(stripped_output)
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(loaded_output, dict):
                return cast(dict[str, object], loaded_output)
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            loaded = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            return cast(dict[str, object], loaded)
    raise RelayError(f"deployment output must include a JSON object: {output!r}")


def _runtime_events(value: object) -> list[dict[str, object]] | None:
    if not isinstance(value, list):
        return None
    raw_items = cast(list[object], value)
    events: list[dict[str, object]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            return None
        events.append(cast(dict[str, object], item))
    return events


def _key_value_output(output: str) -> dict[str, str]:
    if len(output.encode("utf-8")) > 16_384:
        raise RelayError("remote connector start response exceeded its size limit")
    lines = output.splitlines()
    if not lines or len(lines) > 16:
        raise RelayError("remote connector start returned an invalid response")
    values: dict[str, str] = {}
    for line in lines:
        key, separator, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if not separator or not key or not value or key in values:
            raise RelayError("remote connector start returned an invalid key/value response")
        values[key] = value
    return values


def _validated_remote_session_file(
    value: str,
    *,
    session_id: str,
    filename: str,
) -> PurePosixPath:
    """Validate an exact remote session-owned file path without trusting SSH output."""
    if len(value) > 4_096 or any(ord(character) < 32 for character in value):
        raise RelayError("remote connector start returned an invalid owned path")
    path = PurePosixPath(value)
    expected_tail = (
        ".local",
        "share",
        "clio-relay",
        "service-sessions",
        session_id,
        filename,
    )
    if (
        not path.is_absolute()
        or path.as_posix() != value
        or ".." in path.parts
        or tuple(path.parts[-len(expected_tail) :]) != expected_tail
    ):
        raise RelayError("remote connector start returned a path outside its owned session")
    return path


def _connector_step_marker(session_id: str, connector_generation_id: str) -> str:
    """Derive one bounded provider marker from durable connector ownership."""
    digest = hashlib.sha256(f"{session_id}\x00{connector_generation_id}".encode()).hexdigest()[:32]
    return f"clio-relay-connector-{digest}"


def _new_ownership_intent(state: str, **identity: object) -> dict[str, object]:
    """Return one versioned gateway ownership transition record."""
    return {
        "schema_version": _primitives._OWNERSHIP_INTENT_SCHEMA,
        "state": state,
        "updated_at": utc_now().isoformat(),
        **identity,
    }


def _submission_reconciliation_failure_evidence(
    *,
    session_id: str,
    submission_id: str,
    scheduler_provider: str,
    submission_marker: str,
    record: dict[str, object],
    error: str,
    failure_kind: Literal[
        "command_failure",
        "integrity_failure",
        "response_invalid",
    ],
) -> dict[str, object]:
    """Return bounded evidence for one definitive exact-sidecar failure."""

    def bounded_scalar(value: object, *, maximum: int = 256) -> str | int | bool | None:
        if isinstance(value, str):
            return value[:maximum]
        if isinstance(value, (int, bool)):
            return value
        return None

    output = record.get("output")
    output_bytes = output.encode("utf-8") if isinstance(output, str) else None
    recorded_digest = _primitives._optional_str(
        record.get("observed_output_sha256") or record.get("output_sha256")
    )
    reported_identity = _primitives._object(record.get("observed_identity", {}))
    scheduler_submission_outcome = {
        "command_failure": "submit_command_failed",
        "integrity_failure": "unknown_due_to_integrity_failure",
        "response_invalid": "unknown_due_to_invalid_response",
    }[failure_kind]
    return {
        "schema_version": "clio-relay.gateway-submission-reconciliation-failure.v1",
        "session_id": session_id,
        "submission_id": submission_id,
        "scheduler_provider": scheduler_provider,
        "submission_marker": submission_marker,
        "sidecar_present": record.get("present") is True,
        "failure_kind": failure_kind,
        "scheduler_submission_outcome": scheduler_submission_outcome,
        "verification_outcome": bounded_scalar(record.get("verification_outcome")),
        "error_code": bounded_scalar(record.get("error_code")),
        "invalid_component": bounded_scalar(record.get("invalid_component")),
        "observed_identity": {
            field: bounded_scalar(reported_identity.get(field, record.get(field)))
            for field in (
                "schema_version",
                "session_id",
                "submission_id",
                "scheduler_provider",
                "submission_marker",
                "returncode",
                "output_truncated",
            )
        },
        "output_sha256": (recorded_digest[:128] if recorded_digest is not None else None)
        or (hashlib.sha256(output_bytes).hexdigest() if output_bytes is not None else None),
        "output_size": (
            bounded_scalar(record.get("output_size")) if output_bytes is None else len(output_bytes)
        ),
        "error": error[:1024],
        "observed_at": utc_now().isoformat(),
        "cancel_requested": False,
        "resubmit_requested": False,
    }


def _validated_durable_scheduler_contract(
    session: GatewaySession,
    *,
    strict: bool = True,
) -> _types._DurableSchedulerContract:
    """Cross-check scheduler identity or explicit absence across durable records."""
    try:
        spec = ServiceRuntimeSpec.model_validate(session.gateway.get("runtime_spec"))
    except ValueError as exc:
        raise RelayError("owned runtime has no valid service runtime specification") from exc

    binding_document = session.gateway.get("jarvis_runtime_binding")
    if binding_document is not None:
        try:
            binding = JarvisServiceRuntimeBinding.model_validate(binding_document)
        except ValueError as exc:
            raise RelayError("owned runtime has an invalid JARVIS runtime binding") from exc
        provider = binding.scheduler_provider
        scheduler_job_id = binding.scheduler_native_id
        if (provider is None) != (scheduler_job_id is None):
            raise RelayError("JARVIS runtime binding has incomplete scheduler identity")
        expected_provider = provider or "external"
        if session.scheduler != expected_provider or spec.scheduler != expected_provider:
            raise RelayError(
                "scheduler provider disagrees between the gateway, runtime specification, "
                "and JARVIS runtime binding"
            )
        if session.scheduler_job_id != scheduler_job_id:
            raise RelayError(
                "scheduler job identity disagrees between the gateway and JARVIS runtime binding"
            )
        return _types._DurableSchedulerContract(
            provider=expected_provider,
            scheduler_job_id=scheduler_job_id,
        )

    def unresolved_or_known() -> _types._DurableSchedulerContract:
        scheduler_job_id = _primitives._optional_str(session.scheduler_job_id)
        return _types._DurableSchedulerContract(
            provider=session.scheduler,
            scheduler_job_id=scheduler_job_id,
            unresolved_submission=scheduler_job_id is None,
        )

    intents = session.gateway.get("ownership_intents")
    if not isinstance(intents, dict):
        if not strict:
            return unresolved_or_known()
        raise RelayError("gateway has no durable scheduler ownership contract")
    typed_intents = cast(dict[str, object], intents)
    scheduler_intent = typed_intents.get("scheduler_submission")
    if not isinstance(scheduler_intent, dict):
        if not strict:
            return unresolved_or_known()
        raise RelayError("gateway has no durable scheduler submission intent")
    typed_scheduler_intent = cast(dict[str, object], scheduler_intent)
    if typed_scheduler_intent.get("schema_version") != _primitives._OWNERSHIP_INTENT_SCHEMA:
        if not strict:
            return unresolved_or_known()
        raise RelayError("gateway scheduler submission intent has the wrong schema")
    if session.scheduler != spec.scheduler:
        if not strict:
            return unresolved_or_known()
        raise RelayError(
            "scheduler provider disagrees between the gateway and runtime specification"
        )

    state = typed_scheduler_intent.get("state")
    if state in {"not_started", "absent_verified"}:
        if session.scheduler_job_id is not None:
            if not strict:
                return unresolved_or_known()
            raise RelayError(
                "gateway scheduler job identity contradicts an explicit absence intent"
            )
        return _types._DurableSchedulerContract(
            provider=session.scheduler,
            scheduler_job_id=None,
        )

    intent_provider = _primitives._optional_str(typed_scheduler_intent.get("scheduler_provider"))
    if intent_provider != session.scheduler:
        if not strict:
            return unresolved_or_known()
        raise RelayError("scheduler provider disagrees between the gateway and submission intent")
    if state == "starting":
        if (
            session.scheduler_job_id is not None
            or _primitives._optional_str(typed_scheduler_intent.get("submission_id")) is None
            or _primitives._optional_str(typed_scheduler_intent.get("submission_marker")) is None
        ):
            if not strict:
                return unresolved_or_known()
            raise RelayError("starting scheduler submission intent has inconsistent identity")
        return _types._DurableSchedulerContract(
            provider=session.scheduler,
            scheduler_job_id=None,
            unresolved_submission=True,
        )
    if state == "recorded":
        intent_job_id = _primitives._optional_str(typed_scheduler_intent.get("scheduler_job_id"))
        if intent_job_id is None or intent_job_id != session.scheduler_job_id:
            if not strict:
                return unresolved_or_known()
            raise RelayError(
                "scheduler job identity disagrees between the gateway and submission intent"
            )
        return _types._DurableSchedulerContract(
            provider=session.scheduler,
            scheduler_job_id=intent_job_id,
        )
    if not strict:
        return unresolved_or_known()
    raise RelayError("gateway scheduler submission intent has an invalid state")


def _intent_proves_absence(intents: dict[str, object], role: str) -> bool:
    """Return whether a durable intent proves a connector never started or is absent."""
    intent = _primitives._object(intents.get(role, {}))
    return intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA and intent.get(
        "state"
    ) in {
        "not_started",
        "absent_verified",
    }


def _required_intent_str(intent: dict[str, object], field: str) -> str:
    value = _primitives._optional_str(intent.get(field))
    if value is None:
        raise RelayError(f"connector ownership intent has no {field}")
    return value


def _validated_gateway_teardown_intent(
    session: GatewaySession,
    *,
    cancel_scheduler_job: bool,
) -> dict[str, object]:
    """Validate the immutable queue-authored teardown operation identity."""
    raw_intent = session.gateway.get("teardown_intent")
    if not isinstance(raw_intent, dict):
        raise RelayError("gateway teardown intent is invalid")
    intent = cast(dict[str, object], raw_intent)
    if set(intent) != {
        "schema_version",
        "operation_id",
        "gateway_session_id",
        "cancel_scheduler_job",
        "created_at",
    }:
        raise RelayError("gateway teardown intent is invalid")
    operation_id = intent.get("operation_id")
    created_at = intent.get("created_at")
    if (
        intent.get("schema_version") != "clio-relay.gateway-teardown-intent.v1"
        or intent.get("gateway_session_id") != session.session_id
        or not isinstance(operation_id, str)
        or not operation_id.startswith("gateway_cleanup_")
        or not isinstance(created_at, str)
        or not isinstance(intent.get("cancel_scheduler_job"), bool)
    ):
        raise RelayError("gateway teardown intent is invalid")
    _gateway_teardown_timestamp(created_at)
    if intent.get("cancel_scheduler_job") is not cancel_scheduler_job:
        raise RelayError(
            "gateway cleanup policy changed during retry; resume with the original "
            f"cancel_scheduler_job={intent.get('cancel_scheduler_job')} policy"
        )
    return intent


def _validated_gateway_detach_intent(session: GatewaySession) -> dict[str, object]:
    """Validate one immutable relay-authored detach operation identity."""
    raw_intent = session.gateway.get("detach_intent")
    if not isinstance(raw_intent, dict):
        raise RelayError("gateway detach intent is invalid")
    intent = cast(dict[str, object], raw_intent)
    if set(intent) != {
        "schema_version",
        "operation_id",
        "gateway_session_id",
        "created_at",
    }:
        raise RelayError("gateway detach intent is invalid")
    operation_id = intent.get("operation_id")
    created_at = intent.get("created_at")
    if (
        intent.get("schema_version") != _GATEWAY_DETACH_INTENT_SCHEMA
        or intent.get("gateway_session_id") != session.session_id
        or not isinstance(operation_id, str)
        or not operation_id.startswith("gateway_detach_")
        or not isinstance(created_at, str)
    ):
        raise RelayError("gateway detach intent is invalid")
    _gateway_teardown_timestamp(created_at)
    return intent


def _validated_completed_resource_lists(
    result: dict[str, object],
    *,
    error: str,
) -> tuple[list[CleanupResource], list[str]]:
    """Strictly parse bounded completed lifecycle resources and errors."""
    raw_resources = result.get("resources")
    raw_errors = result.get("errors")
    if not isinstance(raw_resources, list) or not isinstance(raw_errors, list):
        raise RelayError(error)
    typed_resources = cast(list[object], raw_resources)
    typed_errors = cast(list[object], raw_errors)
    if not 3 <= len(typed_resources) <= 5 or any(
        not isinstance(item, str) or not item for item in typed_errors
    ):
        raise RelayError(error)
    try:
        resources = [
            CleanupResource.model_validate(resource, strict=True) for resource in typed_resources
        ]
    except ValueError as exc:
        raise RelayError(error) from exc
    return resources, cast(list[str], typed_errors)


def _validate_completed_detach_resources(
    session: GatewaySession,
    *,
    resources: list[CleanupResource],
    stopped_local_pid: int | None,
    operation_id: str,
) -> None:
    """Require complete ownership and disposition proof for a finished detach."""
    error = "gateway detach evidence is invalid"
    scheduler_contract = _validated_durable_scheduler_contract(session)
    allowed_kinds = {
        "browser_proxy",
        "desktop_connector",
        "remote_connector",
        "scheduler_job",
        "scheduler_submission",
        "gateway_record",
    }
    counts = {kind: sum(item.kind == kind for item in resources) for kind in allowed_kinds}
    expected_scheduler_count = 1 if scheduler_contract.scheduler_job_id is not None else 0
    expected_submission_count = 1 if scheduler_contract.unresolved_submission else 0
    if (
        any(item.kind not in allowed_kinds for item in resources)
        or counts["desktop_connector"] != 1
        or counts["remote_connector"] != 1
        or counts["gateway_record"] != 1
        or counts["browser_proxy"] > 1
        or counts["scheduler_job"] != expected_scheduler_count
        or counts["scheduler_submission"] != expected_submission_count
        or any(
            not item.resource_id
            or not item.location
            or item.residual
            or item.metadata.get("gateway_session_id") != session.session_id
            or item.metadata.get("cleanup_operation_id") != operation_id
            or item.metadata.get("cancel_scheduler_job") is not False
            for item in resources
        )
    ):
        raise RelayError(error)
    desktop = next(item for item in resources if item.kind == "desktop_connector")
    remote = next(item for item in resources if item.kind == "remote_connector")
    gateway = next(item for item in resources if item.kind == "gateway_record")
    ownership_intents = _primitives._object(session.gateway.get("ownership_intents", {}))
    remote_absence_proven = _intent_proves_absence(
        ownership_intents,
        "remote_connector",
    )
    if (
        desktop.action != "stop"
        or desktop.outcome not in {"stopped", "missing"}
        or not desktop.ownership_verified
        or not desktop.verified_after_operation
        or (desktop.outcome == "stopped") != (stopped_local_pid is not None)
        or (stopped_local_pid is not None and desktop.resource_id != str(stopped_local_pid))
        or remote.action != "retain"
        or remote.outcome != ("missing" if remote_absence_proven else "retained")
        or not remote.ownership_verified
        or not remote.verified_after_operation
        or (remote_absence_proven and remote.observed_state != "not_created")
        or gateway.resource_id != session.session_id
        or gateway.action != "retain"
        or gateway.outcome != "retained"
        or not gateway.ownership_verified
        or not gateway.verified_after_operation
        or gateway.observed_state != GatewaySessionState.DEGRADED.value
    ):
        raise RelayError(error)
    browser = [item for item in resources if item.kind == "browser_proxy"]
    if browser and (
        browser[0].action != "stop"
        or browser[0].outcome not in {"stopped", "missing"}
        or not browser[0].ownership_verified
        or not browser[0].verified_after_operation
    ):
        raise RelayError(error)
    scheduler = [item for item in resources if item.kind == "scheduler_job"]
    if scheduler:
        item = scheduler[0]
        outcome_state_valid = (
            (item.outcome == "retained" and item.observed_state in _ACTIVE_RUNTIME_STATES)
            or (item.outcome == "terminal" and item.observed_state in _TERMINAL_RUNTIME_STATES)
            or (item.outcome == "missing" and item.observed_state == "missing")
        )
        if (
            item.resource_id != scheduler_contract.scheduler_job_id
            or item.provider != scheduler_contract.provider
            or item.action != "retain"
            or not item.ownership_verified
            or not item.verified_after_operation
            or not outcome_state_valid
        ):
            raise RelayError(error)
    submissions = [item for item in resources if item.kind == "scheduler_submission"]
    if submissions:
        item = submissions[0]
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        scheduler_intent = _primitives._object(intents.get("scheduler_submission", {}))
        if (
            not scheduler_contract.unresolved_submission
            or item.resource_id != scheduler_intent.get("submission_id")
            or item.provider != scheduler_contract.provider
            or item.action != "retain"
            or item.outcome != "retained"
            or item.observed_state != "intent_recorded"
            or not item.ownership_verified
            or not item.verified_after_operation
            or item.metadata.get("submission_id") != scheduler_intent.get("submission_id")
            or item.metadata.get("submission_marker") != scheduler_intent.get("submission_marker")
            or item.metadata.get("scheduler_job_id") is not None
            or item.metadata.get("submission_outcome") != "unresolved"
            or item.metadata.get("cancel_requested") is not False
            or item.metadata.get("resubmit_requested") is not False
        ):
            raise RelayError(error)


def _validate_completed_teardown_resources(
    session: GatewaySession,
    *,
    resources: list[CleanupResource],
    stopped_local_pid: int | None,
    stopped_remote_pid: int | None,
    canceled_scheduler_job: str | None,
    operation_id: str,
    cancel_scheduler_job: bool,
) -> None:
    """Require complete ownership and disposition proof for a finished teardown."""
    error = "completed gateway teardown evidence is invalid"
    scheduler_contract = _validated_durable_scheduler_contract(session)
    if scheduler_contract.unresolved_submission:
        raise RelayError(error)
    allowed_kinds = {
        "browser_proxy",
        "desktop_connector",
        "remote_connector",
        "scheduler_job",
        "gateway_record",
    }
    counts = {kind: sum(item.kind == kind for item in resources) for kind in allowed_kinds}
    expected_scheduler_count = 1 if scheduler_contract.scheduler_job_id is not None else 0
    if (
        any(item.kind not in allowed_kinds for item in resources)
        or counts["desktop_connector"] != 1
        or counts["remote_connector"] != 1
        or counts["gateway_record"] != 1
        or counts["browser_proxy"] > 1
        or counts["scheduler_job"] != expected_scheduler_count
        or any(
            not item.resource_id
            or not item.location
            or item.residual
            or item.metadata.get("gateway_session_id") != session.session_id
            or item.metadata.get("cleanup_operation_id") != operation_id
            or item.metadata.get("cancel_scheduler_job") is not cancel_scheduler_job
            for item in resources
        )
    ):
        raise RelayError(error)
    desktop = next(item for item in resources if item.kind == "desktop_connector")
    remote = next(item for item in resources if item.kind == "remote_connector")
    gateway = next(item for item in resources if item.kind == "gateway_record")
    if (
        desktop.action != "stop"
        or desktop.outcome not in {"stopped", "missing"}
        or not desktop.ownership_verified
        or not desktop.verified_after_operation
        or (desktop.outcome == "stopped") != (stopped_local_pid is not None)
        or (stopped_local_pid is not None and desktop.resource_id != str(stopped_local_pid))
        or remote.action != "stop"
        or remote.outcome not in {"stopped", "missing"}
        or not remote.ownership_verified
        or not remote.verified_after_operation
        or gateway.resource_id != session.session_id
        or gateway.action != "close"
        or gateway.outcome != "closed"
        or not gateway.ownership_verified
        or not gateway.verified_after_operation
    ):
        raise RelayError(error)
    remote_connector = _primitives._object(
        _primitives._object(session.gateway.get("transport", {})).get("remote_connector", {})
    )
    if remote_connector.get("execution_scope") == "scheduler_allocation":
        if stopped_remote_pid is not None or remote.resource_id != _primitives._optional_str(
            remote_connector.get("scheduler_step_id")
        ):
            raise RelayError(error)
    elif (remote.outcome == "stopped") != (stopped_remote_pid is not None) or (
        stopped_remote_pid is not None and remote.resource_id != str(stopped_remote_pid)
    ):
        raise RelayError(error)
    browser = [item for item in resources if item.kind == "browser_proxy"]
    if browser and (
        browser[0].action != "stop"
        or browser[0].outcome not in {"stopped", "missing"}
        or not browser[0].ownership_verified
        or not browser[0].verified_after_operation
    ):
        raise RelayError(error)
    scheduler = [item for item in resources if item.kind == "scheduler_job"]
    if not scheduler:
        if canceled_scheduler_job is not None:
            raise RelayError(error)
        return
    item = scheduler[0]
    if (
        item.resource_id != scheduler_contract.scheduler_job_id
        or item.provider != scheduler_contract.provider
        or not item.ownership_verified
        or not item.verified_after_operation
    ):
        raise RelayError(error)
    if cancel_scheduler_job:
        canceled = item.outcome == "canceled" and item.observed_state in (_CANCELED_RUNTIME_STATES)
        naturally_terminal = (
            item.outcome == "terminal"
            and item.observed_state in _TERMINAL_RUNTIME_STATES - _CANCELED_RUNTIME_STATES
        )
        if (
            item.action != "cancel"
            or not (canceled or naturally_terminal)
            or (canceled_scheduler_job is not None) != canceled
            or (canceled and canceled_scheduler_job != item.resource_id)
        ):
            raise RelayError(error)
        return
    retained_state_valid = (
        (item.outcome == "retained" and item.observed_state in _ACTIVE_RUNTIME_STATES)
        or (item.outcome == "terminal" and item.observed_state in _TERMINAL_RUNTIME_STATES)
        or (item.outcome == "missing" and item.observed_state == "missing")
    )
    if item.action != "retain" or not retained_state_valid or canceled_scheduler_job is not None:
        raise RelayError(error)


def _gateway_teardown_timestamp(value: str) -> datetime:
    """Parse one timezone-aware teardown timestamp without accepting naive evidence."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RelayError("gateway teardown timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RelayError("gateway teardown timestamp is invalid")
    return parsed


def _strict_optional_positive_int(value: object) -> int | None:
    """Validate an optional positive process identity in completed teardown evidence."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RelayError("completed gateway teardown evidence is invalid")
    return value


def _strict_optional_nonempty_str(value: object) -> str | None:
    """Validate an optional non-empty identity in completed teardown evidence."""
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise RelayError("completed gateway teardown evidence is invalid")
    return value


def _completed_teardown_metadata_matches(
    session: GatewaySession,
    *,
    operation_id: str,
    cancel_scheduler_job: bool,
    completed_at: str,
    final_state: GatewaySessionState,
    errors: list[str],
) -> bool:
    """Return whether public session metadata agrees exactly with completed evidence."""
    metadata = session.metadata
    expected_closed_at: str | None = (
        completed_at if final_state is GatewaySessionState.CLOSED else None
    )
    return bool(
        metadata.get("cleanup_at") == completed_at
        and metadata.get("closed_at") == expected_closed_at
        and metadata.get("cancel_scheduler_job") is cancel_scheduler_job
        and metadata.get("cleanup_retryable") is False
        and metadata.get("cleanup_errors") == errors
        and metadata.get("cleanup_operation_id") == operation_id
    )


def _completed_detach_metadata_matches(
    session: GatewaySession,
    *,
    operation_id: str,
    completed_at: str,
    errors: list[str],
) -> bool:
    """Return whether public session metadata agrees with completed detach evidence."""
    metadata = session.metadata
    return bool(
        metadata.get("detached_at") == completed_at
        and metadata.get("detach_operation_id") == operation_id
        and metadata.get("detach_retryable") is False
        and metadata.get("detach_errors") == errors
        and metadata.get("cleanup_retryable") is False
        and metadata.get("cleanup_errors") == errors
    )

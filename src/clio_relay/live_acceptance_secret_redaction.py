"""Secret-free evidence enforcement for the secure runtime acceptance lifecycle.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of proving a
runtime cleanup operation completed exactly as expected, merging its
resources into the canonical report only after ``redact_sensitive_values``
has run, and rejecting any document (or diagnostic exception text) that
still carries a bearer token, browser capability, or other private value.
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal, cast

from clio_relay.errors import RelayError
from clio_relay.models import GatewaySessionState
from clio_relay.service_runtime import ServiceRuntimeStopResult
from clio_relay.validation_report import (
    ValidationRecorder,
    ValidationResource,
    redact_sensitive_values,
)


def _validate_secure_runtime_cleanup(
    result: ServiceRuntimeStopResult,
    *,
    expected_mode: Literal["detach", "teardown"],
    expected_session_id: str,
) -> None:
    """Require exact owned cleanup with scheduler preservation as the default."""
    expected_state = (
        GatewaySessionState.DEGRADED if expected_mode == "detach" else GatewaySessionState.CLOSED
    )
    if (
        result.mode != expected_mode
        or result.session.session_id != expected_session_id
        or result.session.state is not expected_state
        or result.errors
        or result.residual_resources
        or result.canceled_scheduler_job is not None
    ):
        raise RelayError(f"secure runtime {expected_mode} did not complete cleanly")
    if any(resource.action == "cancel" for resource in result.resources):
        raise RelayError(f"secure runtime {expected_mode} requested cancellation")
    report = result.to_live_validation_report()
    if report.status.value != "passed" or report.cleanup.cancel_scheduler_jobs:
        raise RelayError(f"secure runtime {expected_mode} evidence did not pass")


def _record_runtime_cleanup(
    recorder: ValidationRecorder,
    result: ServiceRuntimeStopResult,
    *,
    role: str,
) -> None:
    """Merge one secret-free runtime cleanup operation into the canonical report."""
    recorder.report.cleanup.requested = True
    recorder.report.cleanup.mode = "secure_runtime_detach_reconnect_teardown"
    recorder.report.cleanup.cancel_scheduler_jobs = False
    for resource in result.resources:
        raw = resource.to_validation_resource(cluster=result.session.cluster).model_dump(
            mode="json"
        )
        public = redact_sensitive_values(raw)
        if not isinstance(public, dict):
            raise RelayError("secure runtime cleanup projection was invalid")
        parsed_resource = ValidationResource.model_validate(public)
        validation_resource = parsed_resource.model_copy(
            update={
                "role": role,
                "metadata": {
                    **parsed_resource.metadata,
                    "cancel_scheduler_job": False,
                    "cleanup_action": resource.action,
                    "cleanup_outcome": resource.outcome,
                    "evidence_scope": "clio-relay-core-lifecycle-and-public-evidence",
                },
            }
        )
        recorder.add_resource(validation_resource)
        action = cast(dict[str, Any], redact_sensitive_values(resource.model_dump(mode="json")))
        action["phase"] = role
        if action not in recorder.report.cleanup.actions:
            recorder.report.cleanup.actions.append(action)
        if resource.residual:
            recorder.report.cleanup.remaining_resources.append(validation_resource)


def _assert_secret_free_document(
    document: object,
    *,
    forbidden_values: set[str],
    label: str,
) -> None:
    """Reject raw credentials, browser capabilities, or bearer material in public evidence."""
    rendered = json.dumps(document, ensure_ascii=False, sort_keys=True, default=str)
    if re.search(r"(?i)authorization\s*:\s*bearer\s+(?!<redacted>)", rendered):
        raise RelayError(f"{label} retained a bearer authorization value")
    if "?capability=" in rendered or "&capability=" in rendered:
        raise RelayError(f"{label} retained a browser capability URL")

    def visit(value: object, *, parent_key: str | None = None) -> None:
        if isinstance(value, dict):
            for raw_key, nested in cast(dict[object, object], value).items():
                key = str(raw_key)
                normalized = key.casefold().replace("-", "_").replace(".", "_")
                digest_key = normalized.endswith("_sha256") or normalized.endswith("_digest")
                sensitive_key = normalized in {
                    "authorization",
                    "capability",
                    "credential",
                    "credentials",
                    "password",
                    "private_key",
                    "secret",
                    "secret_key",
                    "token",
                } or normalized.endswith(
                    (
                        "_credential",
                        "_credentials",
                        "_authorization",
                        "_capability",
                        "_password",
                        "_private_key",
                        "_secret",
                        "_secret_key",
                        "_token",
                    )
                )
                if sensitive_key and not digest_key and nested != "<redacted>":
                    raise RelayError(f"{label} retained sensitive field {key}")
                visit(nested, parent_key=key)
        elif isinstance(value, list):
            for nested in cast(list[object], value):
                visit(nested, parent_key=parent_key)
        elif isinstance(value, str):
            if any(secret and secret in value for secret in forbidden_values):
                raise RelayError(f"{label} retained a private capability value")
            if re.search(r"(?i)\bbearer\s+(?!<redacted>)(?:\S+)", value):
                raise RelayError(f"{label} retained bearer authorization material")
            if parent_key == "authorization" and value not in {"<redacted>", "bearer"}:
                raise RelayError(f"{label} retained raw authorization material")

    visit(document)


def _redacted_text(value: str, forbidden_values: set[str]) -> str:
    """Remove known private values from a diagnostic before it reaches a report."""
    result = value
    for secret in sorted(forbidden_values, key=len, reverse=True):
        if secret:
            result = result.replace(secret, "<redacted>")
    return result


def _redacted_error_text(error: BaseException, forbidden_values: set[str]) -> str:
    return _redacted_text(f"{type(error).__name__}: {error}", forbidden_values)


def _redact_exception_values(error: BaseException, forbidden_values: set[str]) -> None:
    """Attach a safe diagnostic when an upstream exception may contain known capabilities."""
    safe = _redacted_error_text(error, forbidden_values)
    if safe != f"{type(error).__name__}: {error}":
        error.add_note(f"redacted secure runtime diagnostic: {safe}")

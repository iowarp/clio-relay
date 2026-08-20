"""Finalized-cleanup-report verification shared by ``session start`` and
``session teardown`` (iowarp/clio-relay#231 continuation): both command
bodies must verify a coordinator-supplied cleanup report against its
recovery status before trusting it, and both bound the public JSON they
echo to the same byte ceiling."""

from __future__ import annotations

import hashlib
from typing import cast

import clio_relay.cli_owned_session_recovery as cli_owned_session_recovery
import clio_relay.cli_owner_session_teardown_verify as cli_owner_session_teardown_verify
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle import (
    OwnedSessionRecoveryStatus,
    SessionLifecycleReport,
    session_lifecycle_report_bytes,
)

MAX_FINALIZED_CLEANUP_RETRY_OUTPUT_BYTES = 1024 * 1024


def _verified_finalized_cleanup_report(
    status: OwnedSessionRecoveryStatus,
    *,
    report: SessionLifecycleReport,
    cluster: str,
    session_id: str,
    expected_generation_id: str | None = None,
    expected_cleanup_operation_id: str | None = None,
    expected_cleanup_policy: dict[str, bool] | None = None,
) -> SessionLifecycleReport:
    """Return only a fully bound and semantically verified coordinator report."""
    generation_id = cli_owned_session_recovery._verified_recovered_owner_session_generation(
        status,
        cluster=cluster,
        session_id=session_id,
    )
    if status.cleanup_paths_pending is not False:
        raise RelayError(
            "owned session cleanup receipt still has pending file deletion; "
            "retry teardown before reconnect"
        )
    if not (
        status.coordinator_report_bound
        and status.coordinator_report is None
        and status.coordinator_report_ref is not None
        and status.coordinator_report_sha256 is not None
    ):
        raise RelayError(
            "owned session cleanup has only cluster-local evidence; retry teardown to "
            "finalize desktop, connector, gateway, relay, and scheduler dispositions"
        )
    report_payload = session_lifecycle_report_bytes(report)
    report_sha256 = hashlib.sha256(report_payload).hexdigest()
    if not (
        len(report_payload) == status.coordinator_report_ref.size
        and report_sha256 == status.coordinator_report_sha256
        and report_sha256 == status.coordinator_report_ref.sha256
    ):
        raise RelayError("coordinator cleanup report size or digest did not match its receipt")
    policy = report.cleanup_policy
    if set(policy) != {"stop_worker", "cancel_jobs", "cancel_scheduler_jobs"}:
        raise RelayError("coordinator cleanup report policy is incomplete")
    if policy["cancel_scheduler_jobs"] and not policy["cancel_jobs"]:
        raise RelayError("coordinator cleanup report has an invalid cancellation policy")
    admission = status.admission_status
    raw_intent = admission.get("cleanup_intent") if isinstance(admission, dict) else None
    intent = cast(dict[str, object], raw_intent) if isinstance(raw_intent, dict) else None
    if not (
        intent is not None
        and report.cleanup_operation_id == intent.get("operation_id")
        and {
            key: intent.get(key) for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
        }
        == policy
    ):
        raise RelayError("coordinator cleanup report does not match immutable cleanup intent")
    if expected_generation_id is not None and generation_id != expected_generation_id:
        raise RelayError("finalized cleanup retry changed its generation identity")
    if (
        expected_cleanup_operation_id is not None
        and report.cleanup_operation_id != expected_cleanup_operation_id
    ):
        raise RelayError("finalized cleanup retry changed its operation identity")
    if expected_cleanup_policy is not None and policy != expected_cleanup_policy:
        raise RelayError("finalized cleanup retry changed its immutable policy")
    cli_owner_session_teardown_verify._verify_owner_session_teardown(
        report,
        session_id=session_id,
        session_generation_id=generation_id,
        stop_worker=policy["stop_worker"],
    )
    return report


def _bounded_cleanup_public_json(value: object) -> str | None:
    """Return public JSON only below the cleanup stdout compatibility boundary."""
    import clio_relay.cli as cli

    serialized = cli._public_json(value)
    return (
        serialized
        if len(serialized.encode("utf-8")) < MAX_FINALIZED_CLEANUP_RETRY_OUTPUT_BYTES
        else None
    )

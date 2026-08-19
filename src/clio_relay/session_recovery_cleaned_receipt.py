"""Terminal pre-metadata failed-and-cleaned receipt inspection (#231 rework).

Extracted from ``session_lifecycle.py``: validates the terminal receipt a
failed owned-session start writes once its own admitted cleanup has
completed, without ever inventing API identity for a start that never
reached a live leader. Called by ``inspect_owned_session_recovery_status``,
which stays resident in ``session_lifecycle.py``.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal, cast

import clio_relay.session_cleanup_targets as session_cleanup_targets
import clio_relay.session_lifecycle_report as session_lifecycle_report
import clio_relay.session_process_scope as session_process_scope
import clio_relay.session_recovery_attempt_status as session_recovery_attempt_status
import clio_relay.session_start_attempt_validation as session_start_attempt_validation
from clio_relay.errors import RelayError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.session_wire_models import (
    MAX_SESSION_START_ERROR_CHARS,
    OwnedSessionCleanupReportReference,
    OwnedSessionInputPolicy,
    OwnedSessionRecoveryStatus,
)

if TYPE_CHECKING:
    from pathlib import Path

    from clio_relay.session_transaction import _OwnedSessionTransaction


def _inspect_owned_session_failed_cleaned_receipt(
    *,
    cluster: str,
    session_id: str,
    document: dict[str, object],
    core_dir: Path,
    transaction: _OwnedSessionTransaction,
    proc_root: Path,
) -> OwnedSessionRecoveryStatus:
    """Validate a terminal pre-metadata cleanup receipt without inventing API identity."""
    from clio_relay.core_queue import ClioCoreQueue
    from clio_relay.process_containment import adopt_linux_systemd_scope_identity
    from clio_relay.session_lifecycle_report import SessionLifecycleReport

    queue = ClioCoreQueue(core_dir)
    errors: list[str] = []
    try:
        attempt = session_start_attempt_validation._validated_start_attempt(
            transaction,
            cluster=cluster,
            session_id=session_id,
        )
    except RelayError as exc:
        attempt = None
        errors.append(str(exc))
    generation = document.get("session_generation_id")
    operation_id = document.get("start_operation_id")
    route_revision = document.get("cluster_route_revision")
    try:
        validated_generation = (
            validate_durable_record_id(generation) if isinstance(generation, str) else None
        )
        validated_operation_id = (
            validate_durable_record_id(operation_id) if isinstance(operation_id, str) else None
        )
    except ValueError:
        validated_generation = None
        validated_operation_id = None

    try:
        report = SessionLifecycleReport.model_validate(document.get("report"))
    except (TypeError, ValueError) as exc:
        report = None
        errors.append(f"failed-start cleanup report is invalid: {exc}")

    coordinator_report_ref: OwnedSessionCleanupReportReference | None = None
    coordinator_report_sha256: str | None = None
    coordinator_report_bound = False
    raw_reference = document.get("coordinator_report_ref")
    if raw_reference is not None:
        try:
            coordinator_report_ref = OwnedSessionCleanupReportReference.model_validate(
                raw_reference
            )
            if validated_generation is None or not isinstance(
                document.get("cleanup_operation_id"), str
            ):
                raise RelayError("failed-start cleanup report reference has no durable identity")
            coordinator_report = session_lifecycle_report._read_coordinator_report_sidecar(
                transaction,
                coordinator_report_ref,
                expected_session_generation_id=validated_generation,
                expected_cleanup_operation_id=cast(str, document["cleanup_operation_id"]),
            )
            if (
                report is None
                or not session_lifecycle_report._coordinator_report_extends_remote_report(
                    coordinator_report,
                    report,
                )
            ):
                raise RelayError("failed-start coordinator report changed the remote report")
            coordinator_report_sha256 = coordinator_report_ref.sha256
            coordinator_report_bound = True
        except (RelayError, TypeError, ValueError) as exc:
            errors.append(f"failed-start coordinator cleanup report is invalid: {exc}")

    try:
        targets = (
            session_cleanup_targets._validate_cleanup_targets(
                document.get("cleanup_targets"),
                generation_id=validated_generation,
            )
            if validated_generation is not None
            else []
        )
    except RelayError as exc:
        targets = []
        errors.append(str(exc))
    cleanup_paths_pending = document.get("cleanup_paths_pending")
    targets_verified = bool(targets)
    if targets and isinstance(cleanup_paths_pending, bool):
        for target in targets:
            observed = transaction.stat_regular(target.name, required=False)
            if cleanup_paths_pending and target.present:
                if observed is None or (
                    observed.st_dev,
                    observed.st_ino,
                    observed.st_size,
                ) != (target.device, target.inode, target.size):
                    targets_verified = False
                    errors.append(f"failed-start cleanup target changed: {target.name}")
                    break
            elif observed is not None:
                targets_verified = False
                errors.append(f"failed-start cleanup target remained unexpectedly: {target.name}")
                break
    else:
        targets_verified = False

    phase = attempt.get("start_phase") if attempt is not None else None
    process_absence_verified = False
    if attempt is not None and phase in {"scope_bound", "contained"}:
        try:
            processes = session_process_scope._recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=cast(str, attempt["systemd_unit"]),
                systemd_cgroup_path=cast(str, attempt["systemd_cgroup_path"]),
                systemd_invocation_id=cast(str, attempt["systemd_invocation_id"]),
                systemd_description=cast(str, attempt["systemd_description"]),
            )
            process_absence_verified = not processes
            if processes:
                errors.append("failed-start owned scope still contains processes")
        except RelayError as exc:
            errors.append(str(exc))
    elif attempt is not None and phase in {"pending", "admitted"}:
        try:
            adopted = adopt_linux_systemd_scope_identity(
                unit=cast(str, attempt["systemd_unit"]),
                description=cast(str, attempt["systemd_description"]),
            )
            process_absence_verified = adopted is None
            if adopted is not None:
                errors.append("failed-start predeclared scope still exists")
        except RuntimeError as exc:
            errors.append(f"failed-start scope absence could not be verified: {exc}")

    admission_status: dict[str, object] | None = None
    durable_generation_verified = False
    raw_policy = document.get("cleanup_policy")
    policy = cast(dict[str, object], raw_policy) if isinstance(raw_policy, dict) else None
    if validated_generation is not None:
        try:
            admission_status = queue.owner_session_generation_status(
                session_id,
                session_generation_id=validated_generation,
            )
            intent = admission_status.get("cleanup_intent")
            expected_intent = cast(dict[str, object], intent) if isinstance(intent, dict) else None
            intent_matches = bool(
                expected_intent is not None
                and expected_intent.get("operation_id") == document.get("cleanup_operation_id")
                and {
                    key: expected_intent.get(key)
                    for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
                }
                == policy
            )
            exact_closing = bool(
                admission_status.get("active_generation_id") == validated_generation
                and admission_status.get("closing_generation_id") == validated_generation
                and admission_status.get("closing") is True
                and admission_status.get("closed") is False
                and intent_matches
            )
            exact_closed = bool(
                admission_status.get("active_generation_id") is None
                and admission_status.get("closing_generation_id") == validated_generation
                and admission_status.get("closing") is True
                and admission_status.get("closed") is True
                and intent_matches
            )
            durable_generation_verified = bool(
                admission_status.get("owner_session_id") == session_id
                and admission_status.get("session_generation_id") == validated_generation
                and (exact_closing or exact_closed)
            )
        except (OSError, RelayError, ValueError) as exc:
            errors.append(f"failed-start admission could not be verified: {exc}")
    if not durable_generation_verified:
        errors.append("failed-start cleanup has no exact closing or closed admission")

    owned_job_ids: list[str] = []
    if validated_generation is not None:
        try:
            owned_job_ids = session_recovery_attempt_status._owned_generation_job_ids(
                queue,
                session_id=session_id,
                session_generation_id=validated_generation,
            )
        except (OSError, RelayError, ValueError) as exc:
            errors.append(f"failed-start owned jobs could not be verified: {exc}")

    expected_keys = {
        "schema_version",
        "owner",
        "cluster",
        "session_id",
        "session_generation_id",
        "start_operation_id",
        "start_phase",
        "failure",
        "remote_api_port",
        "owner_token_sha256",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "systemd_unit",
        "systemd_description",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "process_absence_verified",
        "owned_relay_job_ids",
        "cleanup_operation_id",
        "cleanup_policy",
        "cleanup_paths",
        "cleanup_targets",
        "cleanup_paths_pending",
        "cluster_registry_verified",
        "cluster_registry_removed",
        "completed_at",
        "report",
        "coordinator_report_ref",
    }
    registry_target = next(
        (target for target in targets if target.name.startswith("cluster-registry-")),
        None,
    )
    api_resources = (
        [resource for resource in report.resources if resource.kind == "remote_relay_api"]
        if report is not None
        else []
    )
    file_resources = (
        [resource for resource in report.resources if resource.kind == "remote_session_files"]
        if report is not None
        else []
    )
    raw_completed_at = document.get("completed_at")
    try:
        completed_at = (
            datetime.fromisoformat(raw_completed_at) if isinstance(raw_completed_at, str) else None
        )
    except ValueError:
        completed_at = None
    metadata_verified = bool(
        set(document) == expected_keys
        and document.get("schema_version") == "clio-relay.owner-session-failed-cleaned-receipt.v1"
        and document.get("owner") == "clio-relay"
        and document.get("cluster") == cluster
        and document.get("session_id") == session_id
        and validated_generation is not None
        and validated_operation_id is not None
        and isinstance(route_revision, str)
        and bool(route_revision)
        and attempt is not None
        and attempt.get("session_generation_id") == validated_generation
        and attempt.get("start_operation_id") == validated_operation_id
        and attempt.get("start_phase") == document.get("start_phase")
        and attempt.get("remote_api_port") == document.get("remote_api_port")
        and attempt.get("owner_token_sha256") == document.get("owner_token_sha256")
        and attempt.get("api_release_identity_sha256")
        == document.get("api_release_identity_sha256")
        and attempt.get("cluster_registry_path") == document.get("cluster_registry_path")
        and attempt.get("cluster_registry_sha256") == document.get("cluster_registry_sha256")
        and attempt.get("cluster_route_revision") == route_revision
        and attempt.get("systemd_unit") == document.get("systemd_unit")
        and attempt.get("systemd_description") == document.get("systemd_description")
        and attempt.get("systemd_cgroup_path") == document.get("systemd_cgroup_path")
        and attempt.get("systemd_invocation_id") == document.get("systemd_invocation_id")
        and isinstance(document.get("failure"), str)
        and bool(document.get("failure"))
        and len(cast(str, document.get("failure"))) <= MAX_SESSION_START_ERROR_CHARS
        and document.get("process_absence_verified") is True
        and document.get("owned_relay_job_ids") == owned_job_ids
        and policy is not None
        and set(policy) == {"stop_worker", "cancel_jobs", "cancel_scheduler_jobs"}
        and all(isinstance(value, bool) for value in policy.values())
        and not (
            cast(dict[str, bool], policy)["cancel_scheduler_jobs"]
            and not cast(dict[str, bool], policy)["cancel_jobs"]
        )
        and document.get("cleanup_paths")
        == sorted(
            (
                "api.log",
                "api.pid",
                f"api-startup-{validated_generation}.json",
                f"cluster-registry-{validated_generation}.json",
            )
        )
        and targets_verified
        and registry_target is not None
        and registry_target.present
        and registry_target.sha256 == document.get("cluster_registry_sha256")
        and document.get("cluster_registry_verified") is True
        and isinstance(cleanup_paths_pending, bool)
        and document.get("cluster_registry_removed") is not cleanup_paths_pending
        and completed_at is not None
        and completed_at.tzinfo is not None
        and report is not None
        and report.cluster == cluster
        and report.session_id == session_id
        and report.session_generation_id == validated_generation
        and report.mode == "teardown"
        and report.cleanup_operation_id == document.get("cleanup_operation_id")
        and report.cleanup_policy == policy
        and report.relay_cancel_requested is cast(dict[str, bool], policy)["cancel_jobs"]
        and report.scheduler_cancel_requested
        is cast(dict[str, bool], policy)["cancel_scheduler_jobs"]
        and report.prior_session_status is not None
        and report.prior_session_status.api_pid is None
        and report.prior_session_status.ownership_verified
        and report.post_session_status is not None
        and report.post_session_status.api_pid is None
        and not report.post_session_status.running
        and report.post_session_status.ownership_verified
        and len(api_resources) == 1
        and api_resources[0].metadata.get("failed_start") is True
        and api_resources[0].ownership_verified
        and api_resources[0].verified_after_operation
        and api_resources[0].outcome in {"stopped", "missing"}
        and not api_resources[0].residual
        and len(file_resources) == 1
        and file_resources[0].metadata.get("target_identities")
        == [target.model_dump(mode="json") for target in targets]
        and not report.errors
        and not report.residual_resources
        and (raw_reference is None or coordinator_report_bound)
    )
    if not metadata_verified:
        errors.append("failed-start cleanup receipt identity is invalid")
    recovery_verified = bool(
        metadata_verified
        and process_absence_verified
        and durable_generation_verified
        and not errors
    )
    closed = bool(admission_status is not None and admission_status.get("closed") is True)
    return OwnedSessionRecoveryStatus(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=validated_generation,
        start_operation_id=validated_operation_id,
        cluster_route_revision=route_revision if isinstance(route_revision, str) else None,
        owner="clio-relay" if document.get("owner") == "clio-relay" else None,
        remote_api_port=(
            cast(int, document.get("remote_api_port"))
            if isinstance(document.get("remote_api_port"), int)
            and not isinstance(document.get("remote_api_port"), bool)
            else None
        ),
        leader_process_state="absent" if process_absence_verified else "unverified",
        process_state=(
            "already_closed"
            if recovery_verified and closed
            else "cleanup_pending"
            if recovery_verified
            else "unverified"
        ),
        running=False,
        process_absence_verified=process_absence_verified,
        generation_process_absence_verified=process_absence_verified,
        metadata_verified=metadata_verified,
        cluster_registry_verified=document.get("cluster_registry_verified") is True,
        durable_generation_verified=durable_generation_verified,
        cleanup_receipt=True,
        cleanup_paths_pending=(
            cleanup_paths_pending if isinstance(cleanup_paths_pending, bool) else None
        ),
        coordinator_report=None,
        coordinator_report_ref=(coordinator_report_ref if coordinator_report_bound else None),
        coordinator_report_sha256=(coordinator_report_sha256 if coordinator_report_bound else None),
        coordinator_report_bound=coordinator_report_bound,
        ownership_verified=recovery_verified,
        recovery_verified=recovery_verified,
        api_release_identity_verified=False,
        ownership_token_present=False,
        admission_status=admission_status,
        start_state="failed_cleaned",
        start_phase=cast(
            Literal["pending", "admitted", "scope_bound", "contained"] | None,
            phase,
        ),
        start_attempt_verified=attempt is not None,
        start_retryable=False,
        start_replace=(cast(bool, attempt["replace"]) if attempt is not None else None),
        start_require_token=(cast(bool, attempt["require_token"]) if attempt is not None else None),
        start_input_policy=(
            OwnedSessionInputPolicy.model_validate(attempt["input_policy"])
            if attempt is not None and "input_policy" in attempt
            else None
        ),
        start_expected_api_release_identity_sha256=(
            cast(str | None, attempt["expected_api_release_identity_sha256"])
            if attempt is not None
            else None
        ),
        start_error=(
            cast(str, document.get("failure")) if isinstance(document.get("failure"), str) else None
        ),
        errors=errors,
    )

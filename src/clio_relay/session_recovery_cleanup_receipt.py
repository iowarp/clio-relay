"""Post-metadata cleanup-receipt inspection for idempotent teardown retries (#231 rework).

Extracted from ``session_lifecycle.py``: validates a sanitized cleanup
receipt (written after ``metadata.json`` existed) against its bound
coordinator report -- both the current sidecar-reference form and the
legacy inline form -- so a repeated teardown observes the exact same
terminal state. Called by ``inspect_owned_session_recovery_status``, which
stays resident in ``session_lifecycle.py``.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, cast

import clio_relay.session_cleanup_targets as session_cleanup_targets
import clio_relay.session_lifecycle_report as session_lifecycle_report
import clio_relay.session_process_scope as session_process_scope
from clio_relay.errors import RelayError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.session_lifecycle_report import (
    SessionLifecycleReport,
    session_lifecycle_report_sha256,
)
from clio_relay.session_wire_models import (
    OwnedSessionCleanupReportReference,
    OwnedSessionCleanupTarget,
    OwnedSessionRecoveryStatus,
)

if TYPE_CHECKING:
    from pathlib import Path

    from clio_relay.session_transaction import _OwnedSessionTransaction


def _inspect_owned_session_cleanup_receipt(
    *,
    cluster: str,
    session_id: str,
    document: dict[str, object],
    core_dir: Path,
    proc_root: Path,
    effective_uid: int | None,
    transaction: _OwnedSessionTransaction | None,
) -> OwnedSessionRecoveryStatus:
    """Validate a sanitized receipt for an idempotent teardown retry."""
    from clio_relay.core_queue import ClioCoreQueue

    queue = ClioCoreQueue(core_dir)
    errors: list[str] = []
    generation = document.get("session_generation_id")
    try:
        validated_generation = (
            validate_durable_record_id(generation) if isinstance(generation, str) else None
        )
    except ValueError:
        validated_generation = None
    report: SessionLifecycleReport | None = None
    try:
        report = SessionLifecycleReport.model_validate(document.get("report"))
    except (TypeError, ValueError) as exc:
        errors.append(f"owned session cleanup receipt report is invalid: {exc}")
    coordinator_report: SessionLifecycleReport | None = None
    coordinator_report_ref: OwnedSessionCleanupReportReference | None = None
    coordinator_report_bound = False
    coordinator_report_sha256: object = None
    raw_coordinator_report_ref = document.get("coordinator_report_ref")
    raw_coordinator_report = document.get("coordinator_report")
    legacy_coordinator_sha256 = document.get("coordinator_report_sha256")
    coordinator_fields_valid = bool(
        raw_coordinator_report_ref is None
        and raw_coordinator_report is None
        and legacy_coordinator_sha256 is None
    )
    if raw_coordinator_report_ref is not None:
        try:
            if transaction is None:
                raise RelayError("coordinator cleanup report sidecar has no pinned directory")
            coordinator_report_ref = OwnedSessionCleanupReportReference.model_validate(
                raw_coordinator_report_ref
            )
            if validated_generation is None or not isinstance(
                document.get("cleanup_operation_id"), str
            ):
                raise RelayError("coordinator cleanup report reference has no durable identity")
            coordinator_report = session_lifecycle_report._read_coordinator_report_sidecar(
                transaction,
                coordinator_report_ref,
                expected_session_generation_id=validated_generation,
                expected_cleanup_operation_id=cast(str, document.get("cleanup_operation_id")),
            )
            coordinator_report_sha256 = coordinator_report_ref.sha256
            remote_resources = report.resources if report is not None else []
            coordinator_report_bound = bool(
                report is not None
                and coordinator_report.cluster == report.cluster
                and coordinator_report.session_id == report.session_id
                and coordinator_report.session_generation_id == report.session_generation_id
                and coordinator_report.mode == report.mode
                and coordinator_report.cleanup_operation_id == report.cleanup_operation_id
                and coordinator_report.cleanup_policy == report.cleanup_policy
                and coordinator_report.relay_cancel_requested == report.relay_cancel_requested
                and coordinator_report.scheduler_cancel_requested
                == report.scheduler_cancel_requested
                and coordinator_report.prior_session_status == report.prior_session_status
                and coordinator_report.post_session_status == report.post_session_status
                and len(coordinator_report.resources) >= len(remote_resources)
                and coordinator_report.resources[: len(remote_resources)] == remote_resources
            )
            coordinator_fields_valid = coordinator_report_bound
        except (RelayError, TypeError, ValueError) as exc:
            errors.append(f"owned session coordinator cleanup report is invalid: {exc}")
        if not coordinator_fields_valid:
            errors.append("owned session coordinator cleanup report binding is invalid")
    elif raw_coordinator_report is not None or legacy_coordinator_sha256 is not None:
        # Transitional support for receipts written by the unreleased inline
        # implementation. Status still never returns the resource array.
        try:
            coordinator_report = SessionLifecycleReport.model_validate(raw_coordinator_report)
            observed_coordinator_sha256 = session_lifecycle_report_sha256(coordinator_report)
            remote_resources = report.resources if report is not None else []
            coordinator_report_bound = bool(
                isinstance(legacy_coordinator_sha256, str)
                and legacy_coordinator_sha256 == observed_coordinator_sha256
                and report is not None
                and coordinator_report.cluster == report.cluster
                and coordinator_report.session_id == report.session_id
                and coordinator_report.session_generation_id == report.session_generation_id
                and coordinator_report.mode == report.mode
                and coordinator_report.cleanup_operation_id == report.cleanup_operation_id
                and coordinator_report.cleanup_policy == report.cleanup_policy
                and coordinator_report.relay_cancel_requested == report.relay_cancel_requested
                and coordinator_report.scheduler_cancel_requested
                == report.scheduler_cancel_requested
                and coordinator_report.prior_session_status == report.prior_session_status
                and coordinator_report.post_session_status == report.post_session_status
                and len(coordinator_report.resources) >= len(remote_resources)
                and coordinator_report.resources[: len(remote_resources)] == remote_resources
            )
            coordinator_report_sha256 = legacy_coordinator_sha256
            coordinator_fields_valid = coordinator_report_bound
        except (RelayError, TypeError, ValueError) as exc:
            errors.append(f"owned session legacy coordinator cleanup report is invalid: {exc}")
        if not coordinator_fields_valid:
            errors.append("owned session coordinator cleanup report binding is invalid")
    common_expected_keys = {
        "schema_version",
        "owner",
        "cluster",
        "session_id",
        "session_generation_id",
        "api_pid",
        "api_pgid",
        "remote_api_port",
        "process_start_ticks",
        "owner_token_sha256",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "containment_mode",
        "systemd_unit",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "systemd_description",
        "containment_broker_pid",
        "containment_broker_start_identity",
        "metadata_sha256",
        "cleanup_operation_id",
        "cleanup_policy",
        "cleanup_paths",
        "cleanup_targets",
        "cleanup_paths_pending",
        "cluster_registry_verified",
        "cluster_registry_removed",
        "completed_at",
        "report",
    }
    expected_key_sets = (
        common_expected_keys | {"coordinator_report_ref"},
        common_expected_keys | {"coordinator_report", "coordinator_report_sha256"},
    )
    raw_policy = document.get("cleanup_policy")
    policy = cast(dict[str, object], raw_policy) if isinstance(raw_policy, dict) else None
    completed_at = document.get("completed_at")
    try:
        parsed_completed_at = (
            datetime.fromisoformat(completed_at) if isinstance(completed_at, str) else None
        )
    except ValueError:
        parsed_completed_at = None
    receipt_file_resources = (
        [resource for resource in report.resources if resource.kind == "remote_session_files"]
        if report is not None
        else []
    )
    api_pid = document.get("api_pid")
    api_pgid = document.get("api_pgid")
    remote_api_port = document.get("remote_api_port")
    process_start = document.get("process_start_ticks")
    owner_token_sha256 = document.get("owner_token_sha256")
    release_sha256 = document.get("api_release_identity_sha256")
    registry_path = document.get("cluster_registry_path")
    registry_sha256 = document.get("cluster_registry_sha256")
    route_revision = document.get("cluster_route_revision")
    containment_mode = document.get("containment_mode")
    systemd_unit = document.get("systemd_unit")
    systemd_cgroup_path = document.get("systemd_cgroup_path")
    systemd_invocation_id = document.get("systemd_invocation_id")
    systemd_description = document.get("systemd_description")
    containment_broker_pid = document.get("containment_broker_pid")
    containment_broker_start = document.get("containment_broker_start_identity")
    cleanup_targets_verified = False
    validated_targets: list[OwnedSessionCleanupTarget] = []
    if validated_generation is not None:
        try:
            validated_targets = session_cleanup_targets._validate_cleanup_targets(
                document.get("cleanup_targets"),
                generation_id=validated_generation,
            )
            cleanup_targets_verified = True
        except RelayError as exc:
            errors.append(str(exc))
    metadata_verified = bool(
        set(document) in expected_key_sets
        and document.get("owner") == "clio-relay"
        and document.get("cluster") == cluster
        and document.get("session_id") == session_id
        and validated_generation is not None
        and isinstance(api_pid, int)
        and not isinstance(api_pid, bool)
        and api_pid > 1
        and isinstance(api_pgid, int)
        and not isinstance(api_pgid, bool)
        and api_pgid > 0
        and isinstance(remote_api_port, int)
        and not isinstance(remote_api_port, bool)
        and remote_api_port > 0
        and isinstance(process_start, str)
        and process_start.isdigit()
        and isinstance(owner_token_sha256, str)
        and len(owner_token_sha256) == 64
        and all(character in "0123456789abcdef" for character in owner_token_sha256)
        and isinstance(release_sha256, str)
        and len(release_sha256) == 64
        and all(character in "0123456789abcdef" for character in release_sha256)
        and isinstance(registry_path, str)
        and bool(registry_path)
        and isinstance(registry_sha256, str)
        and len(registry_sha256) == 64
        and all(character in "0123456789abcdef" for character in registry_sha256)
        and isinstance(route_revision, str)
        and bool(route_revision)
        and containment_mode == "linux_systemd_scope"
        and systemd_unit == f"clio-relay-session-{validated_generation}.scope"
        and isinstance(systemd_cgroup_path, str)
        and bool(systemd_cgroup_path)
        and isinstance(systemd_invocation_id, str)
        and len(systemd_invocation_id) == 32
        and all(character in "0123456789abcdef" for character in systemd_invocation_id)
        and isinstance(systemd_description, str)
        and systemd_description.startswith(
            f"clio-relay-owned-session:{session_id}:{validated_generation}:"
        )
        and isinstance(containment_broker_pid, int)
        and not isinstance(containment_broker_pid, bool)
        and containment_broker_pid > 1
        and isinstance(containment_broker_start, str)
        and bool(containment_broker_start)
        and isinstance(document.get("metadata_sha256"), str)
        and len(cast(str, document.get("metadata_sha256"))) == 64
        and all(
            character in "0123456789abcdef"
            for character in cast(str, document.get("metadata_sha256"))
        )
        and document.get("cluster_registry_verified") is True
        and isinstance(document.get("cluster_registry_removed"), bool)
        and isinstance(document.get("cleanup_paths_pending"), bool)
        and document.get("cluster_registry_removed") is not document.get("cleanup_paths_pending")
        and document.get("cleanup_paths")
        == sorted(
            (
                "api.log",
                "api.pid",
                f"api-startup-{validated_generation}.json",
                f"cluster-registry-{validated_generation}.json",
            )
        )
        and cleanup_targets_verified
        and policy is not None
        and set(policy) == {"stop_worker", "cancel_jobs", "cancel_scheduler_jobs"}
        and all(isinstance(value, bool) for value in policy.values())
        and not (policy["cancel_scheduler_jobs"] and not policy["cancel_jobs"])
        and parsed_completed_at is not None
        and parsed_completed_at.tzinfo is not None
        and report is not None
        and report.cluster == cluster
        and report.session_id == session_id
        and report.session_generation_id == validated_generation
        and report.mode == "teardown"
        and report.cleanup_operation_id == document.get("cleanup_operation_id")
        and report.cleanup_policy == policy
        and report.relay_cancel_requested is policy["cancel_jobs"]
        and report.scheduler_cancel_requested is policy["cancel_scheduler_jobs"]
        and report.prior_session_status is not None
        and report.prior_session_status.ownership_verified
        and report.post_session_status is not None
        and report.post_session_status.running is False
        and report.post_session_status.ownership_verified
        and len(receipt_file_resources) == 1
        and receipt_file_resources[0].action == "close"
        and receipt_file_resources[0].outcome == "closed"
        and receipt_file_resources[0].ownership_verified
        and receipt_file_resources[0].verified_after_operation
        and receipt_file_resources[0].metadata.get("metadata_sanitized") is True
        and receipt_file_resources[0].metadata.get("target_identities")
        == [target.model_dump(mode="json") for target in validated_targets]
        and not report.errors
        and not report.residual_resources
        and coordinator_fields_valid
    )
    if not metadata_verified:
        errors.append("owned session cleanup receipt identity is invalid")

    generation_processes: list[session_process_scope._OwnedGenerationProcess] = []
    generation_process_scan_verified = False
    if (
        metadata_verified
        and validated_generation is not None
        and isinstance(systemd_unit, str)
        and isinstance(systemd_cgroup_path, str)
        and isinstance(systemd_invocation_id, str)
        and isinstance(systemd_description, str)
    ):
        try:
            generation_processes = session_process_scope._recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            generation_process_scan_verified = True
        except RelayError as exc:
            errors.append(str(exc))
    if generation_processes:
        errors.append("owned generation processes remain after the cleanup receipt")

    admission_status: dict[str, object] | None = None
    durable_generation_verified = False
    if validated_generation is not None:
        try:
            admission_status = queue.owner_session_generation_status(
                session_id,
                session_generation_id=validated_generation,
            )
            raw_intent = admission_status.get("cleanup_intent")
            intent = cast(dict[str, object], raw_intent) if isinstance(raw_intent, dict) else None
            active_generation = admission_status.get("active_generation_id")
            closing_generation = admission_status.get("closing_generation_id")
            intent_matches = bool(
                intent is not None
                and intent.get("operation_id") == document.get("cleanup_operation_id")
                and {
                    key: intent.get(key)
                    for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
                }
                == document.get("cleanup_policy")
            )
            exact_pending_closure = bool(
                admission_status.get("closing") is True
                and admission_status.get("closed") is False
                and active_generation == validated_generation
                and closing_generation == validated_generation
                and intent_matches
            )
            exact_completed_closure = bool(
                admission_status.get("closing") is True
                and admission_status.get("closed") is True
                and active_generation is None
                and closing_generation == validated_generation
                and intent_matches
            )
            durable_generation_verified = bool(
                admission_status.get("owner_session_id") == session_id
                and admission_status.get("session_generation_id") == validated_generation
                and (exact_pending_closure or exact_completed_closure)
            )
        except (OSError, RelayError, ValueError) as exc:
            errors.append(f"could not verify closed owner-session generation: {exc}")
        if not durable_generation_verified:
            errors.append("cleanup receipt has no exact durable closed-generation proof")

    recovery_verified = bool(
        metadata_verified
        and durable_generation_verified
        and generation_process_scan_verified
        and not generation_processes
        and not errors
    )
    return OwnedSessionRecoveryStatus(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=validated_generation,
        owner="clio-relay" if document.get("owner") == "clio-relay" else None,
        api_pid=api_pid if isinstance(api_pid, int) and not isinstance(api_pid, bool) else None,
        remote_api_port=(
            remote_api_port
            if isinstance(remote_api_port, int) and not isinstance(remote_api_port, bool)
            else None
        ),
        process_start_marker=process_start if isinstance(process_start, str) else None,
        leader_process_state="absent" if recovery_verified else "unverified",
        process_state=(
            "owned_running"
            if generation_processes
            else "already_closed"
            if recovery_verified and admission_status is not None and admission_status.get("closed")
            else "cleanup_pending"
            if recovery_verified
            else "unverified"
        ),
        running=bool(generation_processes),
        process_absence_verified=generation_process_scan_verified and not generation_processes,
        generation_process_pids=[process.pid for process in generation_processes],
        generation_process_absence_verified=(
            generation_process_scan_verified and not generation_processes
        ),
        metadata_verified=metadata_verified,
        cluster_registry_verified=document.get("cluster_registry_verified") is True,
        durable_generation_verified=durable_generation_verified,
        cleanup_receipt=True,
        cleanup_paths_pending=(
            cast(bool, document.get("cleanup_paths_pending"))
            if isinstance(document.get("cleanup_paths_pending"), bool)
            else None
        ),
        coordinator_report=None,
        coordinator_report_ref=(coordinator_report_ref if coordinator_report_bound else None),
        coordinator_report_sha256=(
            coordinator_report_sha256
            if coordinator_report_bound and isinstance(coordinator_report_sha256, str)
            else None
        ),
        coordinator_report_bound=coordinator_report_bound,
        ownership_verified=recovery_verified,
        recovery_verified=recovery_verified,
        api_release_identity_verified=bool(
            metadata_verified and generation_process_scan_verified and not generation_processes
        ),
        ownership_token_present=False,
        admission_status=admission_status,
        errors=errors,
    )

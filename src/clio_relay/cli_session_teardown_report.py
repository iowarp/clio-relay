"""``session teardown``'s report-emission phase (iowarp/clio-relay#231
continuation, ``cli_session_teardown.py`` split): the three functions
that turn a coordinator ``SessionLifecycleReport`` into the bounded
validation-report/evidence-lock-checkpointed output the command emits,
at each of its three emission points -- a durable checkpoint before
authoritative closure, the normal completed-cleanup report, and the
compact finalized-retry report used both for an already-closed retry
and as the completed report's own fallback when the full projection
would exceed the output's byte ceiling.

These three were nested closures sharing ~15 enclosing local variables
(cluster/session identity, the canonical report path, the evidence
lock, the worker-observation evidence) in the pre-split module; they
move here as top-level functions taking the same shared
:class:`~clio_relay.cli_session_teardown_state._TeardownState` object
every other phase function takes, in place of those closures' free
variables. Function bodies are otherwise unchanged from the pre-split
module.
"""

from __future__ import annotations

import json
from collections import Counter

import typer

import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence
import clio_relay.cli_cleanup_report as cli_cleanup_report
import clio_relay.cli_owned_report_artifact as cli_owned_report_artifact
import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach
import clio_relay.validation_report as validation_report_module
from clio_relay.cli_session_teardown_state import _TeardownState
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle import (
    CleanupResource,
    OwnedSessionRecoveryStatus,
    SessionLifecycleReport,
)
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    load_validation_report,
    redact_sensitive_values,
    sha256_file,
)

MAX_CLEANUP_VALIDATION_REPORT_BYTES = 8 * 1024 * 1024


def _checkpoint_finalized_cleanup_artifact(
    state: _TeardownState,
    report: SessionLifecycleReport,
    *,
    recovery: OwnedSessionRecoveryStatus,
    local_artifact: cli_cleanup_evidence._LocalCleanupReportArtifact,
) -> None:
    """Durably reference exact local evidence before authoritative closure."""
    cli_cleanup_evidence._verify_cleanup_evidence_lock(
        state.active_evidence_lock,
        expected_parent=cli_cleanup_evidence._cleanup_evidence_state_parent(),
    )
    reference = recovery.coordinator_report_ref
    generation_id = report.session_generation_id
    operation_id = report.cleanup_operation_id
    if not (
        reference is not None
        and generation_id is not None
        and operation_id is not None
        and reference.sha256 == local_artifact.report_sha256
        and reference.size == local_artifact.report_size
    ):
        raise RelayError("cleanup report artifact does not match its finalized reference")
    report_metadata: dict[str, object] = {
        "sha256": reference.sha256,
        "size": reference.size,
        "local_manifest": str(local_artifact.manifest_path.resolve()),
        "local_manifest_sha256": local_artifact.manifest_sha256,
        "local_chunk_count": len(local_artifact.chunks),
        "session_generation_id": generation_id,
        "cleanup_operation_id": operation_id,
        "cleanup_policy": report.cleanup_policy,
    }
    pending = state.seed_report.model_copy(
        deep=True,
        update={
            "checks": [],
            "resources": [],
            "artifacts": [],
            "completed_at": None,
            "status": ValidationStatus.FAILED,
            "error": "authoritative owner-session closure pending",
            "cleanup": CleanupEvidence(
                requested=True,
                mode="teardown",
                operation_id=operation_id,
                cancel_relay_jobs=report.cleanup_policy["cancel_jobs"],
                cancel_scheduler_jobs=report.cleanup_policy["cancel_scheduler_jobs"],
                stop_worker=report.cleanup_policy["stop_worker"],
                actions=[
                    {
                        "kind": "cleanup_report",
                        "resource_id": reference.name,
                        "action": "verify",
                        "outcome": "verified",
                        "verified_after_operation": True,
                        "residual": False,
                    },
                    {
                        "kind": "owner_session",
                        "resource_id": f"{state.session_id}:{generation_id}",
                        "action": "close",
                        "outcome": "pending",
                        "verified_after_operation": False,
                        "residual": True,
                    },
                ],
                remaining_resources=[
                    ValidationResource(
                        kind="owner_session",
                        resource_id=f"{state.session_id}:{generation_id}",
                        role="cleanup_closure",
                        cluster=state.cluster,
                        state="pending",
                        metadata={"cleanup_operation_id": operation_id},
                    )
                ],
            ),
        },
    )
    recorder = ValidationRecorder(pending)
    manifest_evidence = EvidenceReference(
        kind="cleanup_report_manifest",
        reference=str(local_artifact.manifest_path.resolve()),
        sha256=local_artifact.manifest_sha256,
        metadata=report_metadata,
    )
    with recorder.check(
        "session.teardown.cleanup-report-retained",
        "retain exact finalized cleanup report before authoritative closure",
    ) as evidence:
        evidence.append(manifest_evidence)
    pending.artifacts.append(manifest_evidence)
    pending.artifacts.extend(
        EvidenceReference(
            kind="cleanup_report_chunk",
            reference=str(chunk.path.resolve()),
            sha256=chunk.sha256,
            metadata={"size": chunk.size},
        )
        for chunk in local_artifact.chunks
    )
    recorder.add_resource(
        ValidationResource(
            kind="cleanup_report",
            resource_id=reference.name,
            role="finalized_cleanup_report",
            cluster=state.cluster,
            state="verified",
            references=[str(local_artifact.manifest_path.resolve())],
            metadata=report_metadata,
        )
    )
    recorder.add_resource(
        ValidationResource(
            kind="owner_session",
            resource_id=f"{state.session_id}:{generation_id}",
            role="cleanup_closure",
            cluster=state.cluster,
            state="pending",
            metadata={"cleanup_operation_id": operation_id},
        )
    )
    validation_report_module.write_validation_report(pending, state.canonical_report_path)
    cli_cleanup_evidence._verify_cleanup_evidence_lock(
        state.active_evidence_lock,
        expected_parent=cli_cleanup_evidence._cleanup_evidence_state_parent(),
    )
    reread = load_validation_report(state.canonical_report_path)
    cli_cleanup_evidence._verify_cleanup_evidence_lock(
        state.active_evidence_lock,
        expected_parent=cli_cleanup_evidence._cleanup_evidence_state_parent(),
    )
    expected_checkpoint = LiveValidationReport.model_validate(
        redact_sensitive_values(pending.model_dump(mode="json"))
    )
    if reread.model_dump(mode="json") != expected_checkpoint.model_dump(mode="json"):
        raise RelayError("cleanup report artifact checkpoint changed during durable re-read")
    state.canonical_report = reread


def _emit_completed_report(
    state: _TeardownState,
    report: SessionLifecycleReport,
    *,
    canceled_job_ids: list[str],
    gateway_reports: list[dict[str, object]],
    recovery: OwnedSessionRecoveryStatus,
    local_artifact: cli_cleanup_evidence._LocalCleanupReportArtifact,
    legacy_recovery: OwnedSessionRecoveryStatus | None,
) -> None:
    """Keep the bounded legacy projection, falling back to compact evidence."""
    generation_id = report.session_generation_id
    operation_id = report.cleanup_operation_id
    reference = recovery.coordinator_report_ref
    if generation_id is None or operation_id is None or reference is None:
        raise RelayError("finalized cleanup omitted its durable identity")
    projection = report.model_copy(deep=True)
    projection.resources.append(
        CleanupResource(
            kind="owner_session",
            resource_id=f"{state.session_id}:{generation_id}",
            location=(
                state.definition.ssh_host if state.remote_execution else str(state.queue.root)
            ),
            action="close",
            ownership_verified=True,
            outcome="closed",
            verified_after_operation=True,
            metadata={
                "session_generation_id": generation_id,
                "cleanup_operation_id": operation_id,
                "cleanup_policy": report.cleanup_policy,
                "covered_legacy_job_ids": [],
            },
        )
    )
    payload = projection.json_payload()
    payload["cleanup_evidence"] = projection.to_cleanup_evidence(
        stop_worker=state.stop_worker
    ).model_dump(mode="json")
    payload["relay_jobs"] = {
        "cancel_requested": state.cancel_jobs,
        "scheduler_cancel_requested": state.cancel_jobs and state.cancel_scheduler_jobs,
        "canceled_job_ids": canceled_job_ids,
    }
    payload["gateway_sessions"] = gateway_reports
    if legacy_recovery is not None:
        payload["recovery_evidence"] = legacy_recovery.model_dump(mode="json")
    payload.update(
        {
            "validation_report": str(state.canonical_report_path.resolve()),
            "validation_status": ValidationStatus.PASSED.value,
            "validation_provenance_warning": False,
        }
    )
    preliminary = cli_cleanup_report._bounded_cleanup_public_json(payload)
    if preliminary is not None:
        canonical = projection.to_live_validation_report(
            stop_worker=state.stop_worker,
            cancel_jobs=state.cancel_jobs,
            launcher=state.validation_launcher,
            install_source=state.validation_install_source,
            artifact_sha256=(
                sha256_file(state.validation_artifact)
                if state.validation_artifact is not None
                else None
            ),
        ).model_copy(
            update={
                "report_id": state.seed_report.report_id,
                "started_at": state.seed_report.started_at,
            }
        )
        report_metadata: dict[str, object] = {
            "sha256": reference.sha256,
            "size": reference.size,
            "local_manifest": str(local_artifact.manifest_path.resolve()),
            "local_manifest_sha256": local_artifact.manifest_sha256,
            "local_chunk_count": len(local_artifact.chunks),
            "session_generation_id": generation_id,
            "cleanup_operation_id": operation_id,
            "cleanup_policy": report.cleanup_policy,
        }
        manifest_evidence = EvidenceReference(
            kind="cleanup_report_manifest",
            reference=str(local_artifact.manifest_path.resolve()),
            sha256=local_artifact.manifest_sha256,
            metadata=report_metadata,
        )
        canonical.artifacts.append(manifest_evidence)
        canonical.artifacts.extend(
            EvidenceReference(
                kind="cleanup_report_chunk",
                reference=str(chunk.path.resolve()),
                sha256=chunk.sha256,
                metadata={"size": chunk.size},
            )
            for chunk in local_artifact.chunks
        )
        canonical.resources.append(
            ValidationResource(
                kind="cleanup_report",
                resource_id=reference.name,
                role="finalized_cleanup_report",
                cluster=state.cluster,
                state="verified",
                references=[str(local_artifact.manifest_path.resolve())],
                metadata=report_metadata,
            )
        )
        projected_validation = (
            json.dumps(
                redact_sensitive_values(canonical.model_dump(mode="json")),
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        if len(projected_validation.encode("utf-8")) < MAX_CLEANUP_VALIDATION_REPORT_BYTES:
            state.canonical_report = canonical
            provenance_warning = cli_remote_worker_attach._write_cleanup_validation_report(
                canonical,
                state.definition,
                state.canonical_report_path,
                observed_worker_info=state.cleanup_worker_info,
                worker_observation_error=state.cleanup_worker_error,
            )
            payload["validation_status"] = canonical.status.value
            payload["validation_provenance_warning"] = provenance_warning
            serialized = cli_cleanup_report._bounded_cleanup_public_json(payload)
            if (
                serialized is not None
                and state.canonical_report_path.stat().st_size < MAX_CLEANUP_VALIDATION_REPORT_BYTES
            ):
                typer.echo(serialized)
                canonical_ok = canonical.status is ValidationStatus.PASSED
                if payload.get("ok") is not True or (not canonical_ok and not provenance_warning):
                    raise typer.Exit(code=1)
                return
    _emit_finalized_retry_report(
        state,
        report,
        recovery=recovery,
        local_artifact=local_artifact,
        retry=False,
    )


def _emit_finalized_retry_report(
    state: _TeardownState,
    report: SessionLifecycleReport,
    *,
    recovery: OwnedSessionRecoveryStatus,
    local_artifact: cli_cleanup_evidence._LocalCleanupReportArtifact,
    retry: bool = True,
) -> None:
    """Emit bounded evidence for a finalized report without re-inlining it."""
    reference = recovery.coordinator_report_ref
    generation_id = report.session_generation_id
    operation_id = report.cleanup_operation_id
    admission = recovery.admission_status
    if not (
        reference is not None
        and generation_id is not None
        and operation_id is not None
        and isinstance(admission, dict)
        and admission.get("closed") is True
        and recovery.process_state == "already_closed"
    ):
        raise RelayError("finalized cleanup retry omitted authoritative closure evidence")

    resource_summary: dict[str, object] = {
        "total": len(report.resources),
        "by_kind": dict(sorted(Counter(item.kind for item in report.resources).items())),
        "by_action": dict(sorted(Counter(item.action for item in report.resources).items())),
        "by_outcome": dict(sorted(Counter(item.outcome for item in report.resources).items())),
        "residual_count": len(report.residual_resources),
        "error_count": len(report.errors),
    }
    canceled_relay_count = sum(
        1
        for resource in report.resources
        if resource.kind == "relay_job"
        and resource.action == "cancel"
        and resource.outcome == "canceled"
        and resource.ownership_verified
        and resource.verified_after_operation
        and not resource.residual
    )
    gateway_resource_count = sum(
        1
        for resource in report.resources
        if resource.kind in {"desktop_connector", "remote_connector", "gateway_record"}
    )
    report_metadata: dict[str, object] = {
        "sha256": reference.sha256,
        "size": reference.size,
        "local_manifest": str(local_artifact.manifest_path.resolve()),
        "local_manifest_sha256": local_artifact.manifest_sha256,
        "local_chunk_count": len(local_artifact.chunks),
        "session_generation_id": generation_id,
        "cleanup_operation_id": operation_id,
        "cleanup_policy": report.cleanup_policy,
        "resource_summary": resource_summary,
    }
    canonical = state.seed_report.model_copy(
        deep=True,
        update={
            "checks": [],
            "resources": [],
            "artifacts": [],
            "completed_at": None,
            "status": ValidationStatus.FAILED,
            "error": None,
            "cleanup": CleanupEvidence(
                requested=True,
                mode="teardown",
                operation_id=operation_id,
                cancel_relay_jobs=report.cleanup_policy["cancel_jobs"],
                cancel_scheduler_jobs=report.cleanup_policy["cancel_scheduler_jobs"],
                stop_worker=report.cleanup_policy["stop_worker"],
                actions=[
                    {
                        "kind": "cleanup_report",
                        "resource_id": reference.name,
                        "action": "verify",
                        "outcome": "verified",
                        "verified_after_operation": True,
                        "residual": False,
                    },
                    {
                        "kind": "owner_session",
                        "resource_id": f"{state.session_id}:{generation_id}",
                        "action": "close",
                        "outcome": "closed",
                        "verified_after_operation": True,
                        "residual": False,
                    },
                ],
                remaining_resources=[],
            ),
        },
    )
    recorder = ValidationRecorder(canonical)
    manifest_evidence = EvidenceReference(
        kind="cleanup_report_manifest",
        reference=str(local_artifact.manifest_path.resolve()),
        sha256=local_artifact.manifest_sha256,
        metadata=report_metadata,
    )
    with recorder.check(
        ("session.teardown.finalized-retry" if retry else "session.teardown.finalized"),
        "verify finalized cleanup report and authoritative session closure",
    ) as evidence:
        evidence.append(manifest_evidence)
    canonical.artifacts.append(manifest_evidence)
    canonical.artifacts.extend(
        EvidenceReference(
            kind="cleanup_report_chunk",
            reference=str(chunk.path.resolve()),
            sha256=chunk.sha256,
            metadata={"size": chunk.size},
        )
        for chunk in local_artifact.chunks
    )
    recorder.add_resource(
        ValidationResource(
            kind="cleanup_report",
            resource_id=reference.name,
            role="finalized_cleanup_report",
            cluster=state.cluster,
            state="verified",
            references=[str(local_artifact.manifest_path.resolve())],
            metadata=report_metadata,
        )
    )
    recorder.add_resource(
        ValidationResource(
            kind="owner_session_recovery",
            resource_id=f"{state.session_id}:{generation_id}",
            role="post_cleanup_recovery",
            cluster=state.cluster,
            state="verified",
            metadata={
                "process_state": recovery.process_state,
                "cleanup_receipt": recovery.cleanup_receipt,
                "cleanup_paths_pending": recovery.cleanup_paths_pending,
                "coordinator_report_bound": recovery.coordinator_report_bound,
                "ownership_verified": recovery.ownership_verified,
                "recovery_verified": recovery.recovery_verified,
                "closed": True,
            },
        )
    )
    recorder.finish()
    state.canonical_report = canonical
    provenance_warning = cli_remote_worker_attach._write_cleanup_validation_report(
        canonical,
        state.definition,
        state.canonical_report_path,
        observed_worker_info=state.cleanup_worker_info,
        worker_observation_error=state.cleanup_worker_error,
    )
    if state.canonical_report_path.stat().st_size >= MAX_CLEANUP_VALIDATION_REPORT_BYTES:
        raise RelayError("finalized cleanup validation report exceeded its byte limit")
    payload: dict[str, object] = {
        "schema_version": (
            "clio-relay.finalized-cleanup-retry.v1" if retry else "clio-relay.finalized-cleanup.v1"
        ),
        "cluster": state.cluster,
        "session_id": state.session_id,
        "session_generation_id": generation_id,
        "mode": report.mode,
        "cleanup_operation_id": operation_id,
        "cleanup_policy": report.cleanup_policy,
        "coordinator_report_ref": reference.model_dump(mode="json"),
        "coordinator_report_sha256": reference.sha256,
        "report_inline": False,
        "cleanup_report_artifact": {
            "manifest": str(local_artifact.manifest_path.resolve()),
            "manifest_sha256": local_artifact.manifest_sha256,
            "report_sha256": local_artifact.report_sha256,
            "report_size": local_artifact.report_size,
            "chunk_count": len(local_artifact.chunks),
            "chunk_size_limit": (cli_owned_report_artifact.MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES),
        },
        "resource_summary": resource_summary,
        "relay_jobs": {
            "cancel_requested": report.cleanup_policy["cancel_jobs"],
            "scheduler_cancel_requested": report.cleanup_policy["cancel_scheduler_jobs"],
            "canceled_count": canceled_relay_count,
        },
        "gateway_sessions": {"resource_count": gateway_resource_count},
        "authoritative_closure": True,
        "recovery_evidence": {
            "process_state": recovery.process_state,
            "closed": True,
            "cleanup_receipt": recovery.cleanup_receipt,
            "cleanup_paths_pending": recovery.cleanup_paths_pending,
            "coordinator_report_bound": recovery.coordinator_report_bound,
            "coordinator_report_ref": reference.model_dump(mode="json"),
            "ownership_verified": recovery.ownership_verified,
            "recovery_verified": recovery.recovery_verified,
        },
        "validation_report": str(state.canonical_report_path.resolve()),
        "validation_status": canonical.status.value,
        "validation_provenance_warning": provenance_warning,
        "ok": True,
    }
    serialized = cli_cleanup_report._bounded_cleanup_public_json(payload)
    if serialized is None:
        raise RelayError("finalized cleanup output exceeded its byte limit")
    typer.echo(serialized)
    if canonical.status is not ValidationStatus.PASSED and not provenance_warning:
        raise typer.Exit(code=1)

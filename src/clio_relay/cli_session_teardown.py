"""The ``session teardown`` command (iowarp/clio-relay#231 continuation).

This is the single largest command body in clio-relay: a deeply nested
closure factory (``action`` -> ``checkpoint_finalized_cleanup_artifact``/
``emit_completed_report``/``emit_finalized_retry_report`` ->
``guarded_action`` -> ``locked_action``) that threads ~20 enclosing
local variables (the evidence lock, the mutable ``canonical_report``
cell, cluster/session identity, the requested cleanup policy) through
its inner functions via Python closures rather than explicit
parameters. Splitting those closures into standalone top-level
functions would mean converting each one into an explicit multi-
parameter API -- a semantic rewrite of security-sensitive
cleanup-evidence-locked code, not a verbatim move, and out of scope
for this extraction's behavior-identical guarantee. The command moves
here as one atomic, unsplit unit; see
``scripts/check_file_size.py``'s ``RATCHET_BASELINE`` entry for this
file, which records the same rationale."""

from __future__ import annotations

import json
from collections import Counter
from json import JSONDecodeError
from pathlib import Path
from typing import Annotated, cast

import typer

import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence
import clio_relay.cli_cleanup_report as cli_cleanup_report
import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.cli_owned_report_artifact as cli_owned_report_artifact
import clio_relay.cli_owned_runtime_cleanup as cli_owned_runtime_cleanup
import clio_relay.cli_owned_scheduler_cancel as cli_owned_scheduler_cancel
import clio_relay.cli_owned_session_recovery as cli_owned_session_recovery
import clio_relay.cli_owner_session_teardown_verify as cli_owner_session_teardown_verify
import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach
import clio_relay.cli_session as cli_session
import clio_relay.cli_support as cli_support
import clio_relay.remote_cli as remote_cli
import clio_relay.session_lifecycle as session_lifecycle
import clio_relay.validation_report as validation_report_module
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.errors import RelayError
from clio_relay.models import (
    JobState,
)
from clio_relay.owner_session_admission import (
    assert_no_unscoped_desktop_admission_state as _assert_no_unscoped_desktop_admission_state,
)
from clio_relay.owner_session_admission import (
    desktop_owner_session_admission_id as _desktop_owner_session_admission_id,
)
from clio_relay.session_lifecycle import (
    CleanupResource,
    OwnedSessionRecoveryStatus,
    SessionLifecycleReport,
    session_lifecycle_report_sha256,
)
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    default_report_path,
    load_validation_report,
    redact_sensitive_values,
    sha256_file,
)

DEFAULT_RELAY_CANCEL_TIMEOUT_SECONDS = 30.0


DEFAULT_RELAY_CANCEL_POLL_SECONDS = 0.25


MAX_RELAY_CANCEL_TIMEOUT_SECONDS = 3_600.0


MAX_CLEANUP_VALIDATION_REPORT_BYTES = 8 * 1024 * 1024


def _persist_verified_cleanup_report_before_closure(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    report: SessionLifecycleReport,
) -> tuple[SessionLifecycleReport, OwnedSessionRecoveryStatus]:
    """Persist, re-read, and verify the immutable full cleanup report."""
    cleanup_operation_id = report.cleanup_operation_id
    if cleanup_operation_id is None:
        raise RelayError("coordinator cleanup report omitted its operation id")
    finalized_status = session_lifecycle.finalize_remote_session_cleanup_report(
        definition=definition,
        cluster=cluster,
        session_id=session_id,
        session_generation_id=session_generation_id,
        cleanup_operation_id=cleanup_operation_id,
        cleanup_policy=report.cleanup_policy,
        report=report,
    )
    retrieved_report = session_lifecycle.read_remote_session_cleanup_report(
        definition=definition,
        cluster=cluster,
        session_id=session_id,
        status=finalized_status,
    )
    finalized_report = cli_cleanup_report._verified_finalized_cleanup_report(
        finalized_status,
        report=retrieved_report,
        cluster=cluster,
        session_id=session_id,
        expected_generation_id=session_generation_id,
        expected_cleanup_operation_id=cleanup_operation_id,
        expected_cleanup_policy=report.cleanup_policy,
    )
    if session_lifecycle_report_sha256(finalized_report) != session_lifecycle_report_sha256(report):
        raise RelayError("re-read coordinator cleanup report changed before closure")
    return finalized_report, finalized_status


@cli_session.session_app.command("teardown")
@cli_support._acceptance_report_command
def session_teardown(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    stop_worker: Annotated[
        bool,
        typer.Option(help="Also stop the persistent cluster worker service for this cluster."),
    ] = False,
    cancel_jobs: Annotated[
        bool,
        typer.Option(
            "--cancel-jobs/--keep-jobs",
            help="Cancel active relay jobs. The safe default leaves all jobs running.",
        ),
    ] = False,
    cancel_scheduler_jobs: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-jobs/--keep-scheduler-jobs",
            help="Also request scheduler cancellation for canceled relay jobs.",
        ),
    ] = False,
    preserve_scheduler_job_ids: Annotated[
        list[str] | None,
        typer.Option(
            "--preserve-scheduler-job-id",
            help=(
                "Unrelated active scheduler job id that must remain uncanceled; repeat for "
                "multiple live-gate sentinels. Requires --cancel-jobs and "
                "--cancel-scheduler-jobs."
            ),
        ),
    ] = None,
    relay_cancel_timeout_seconds: Annotated[
        float,
        typer.Option(
            help="Maximum wait for worker-acknowledged relay cancellation cleanup.",
            min=0.01,
            max=MAX_RELAY_CANCEL_TIMEOUT_SECONDS,
        ),
    ] = DEFAULT_RELAY_CANCEL_TIMEOUT_SECONDS,
    relay_cancel_poll_seconds: Annotated[
        float,
        typer.Option(
            help="Polling interval while awaiting relay cancellation acknowledgment.",
            min=0.01,
            max=60.0,
        ),
    ] = DEFAULT_RELAY_CANCEL_POLL_SECONDS,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical cleanup validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in cleanup evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop owned remote relay session processes, optionally stopping the worker service."""
    import clio_relay.cli as cli

    canonical_report_path = validation_report or default_report_path(cluster)
    evidence_lock: cli_cleanup_evidence._CleanupEvidenceLock | None = None
    try:
        evidence_lock = cli_cleanup_evidence._acquire_cleanup_evidence_lock()
        seed_report = cli_remote_worker_attach._new_cleanup_acceptance_report(
            scenario="cleanup",
            cluster=cluster,
            mode="teardown",
            resource_kind="owner_session",
            resource_id=session_id,
            action="teardown",
            cancel_relay_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            stop_worker=stop_worker,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        canonical_report: list[LiveValidationReport | None] = [seed_report]
        validation_report_module.write_validation_report(seed_report, canonical_report_path)
    except BaseException:
        cli_cleanup_evidence._release_cleanup_evidence_lock(evidence_lock)
        raise
    active_evidence_lock = evidence_lock
    try:
        definition = cli._require_cluster(cluster)
        scheduler_sentinel_ids = cli_owned_scheduler_cancel._normalize_scheduler_sentinel_ids(
            preserve_scheduler_job_ids or []
        )
        if cancel_scheduler_jobs and not cancel_jobs:
            raise typer.BadParameter(
                "--cancel-scheduler-jobs requires the separate --cancel-jobs flag"
            )
        if scheduler_sentinel_ids and not (cancel_jobs and cancel_scheduler_jobs):
            raise typer.BadParameter(
                "--preserve-scheduler-job-id requires both --cancel-jobs and "
                "--cancel-scheduler-jobs"
            )
    except BaseException as exc:
        try:
            cli._write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="cleanup",
                cluster=cluster,
                check_id="session.teardown.preflight",
                summary="validate owned session teardown inputs",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
        finally:
            cli_cleanup_evidence._release_cleanup_evidence_lock(evidence_lock)
        raise

    def action() -> None:
        remote_execution = remote_cli.should_execute_on_cluster(definition)
        queue = cli._managed_queue_from_env()
        cleanup_worker_info, cleanup_worker_error = (
            cli_remote_worker_attach._observe_worker_before_cleanup(definition)
        )

        def checkpoint_finalized_cleanup_artifact(
            report: SessionLifecycleReport,
            *,
            recovery: OwnedSessionRecoveryStatus,
            local_artifact: cli_cleanup_evidence._LocalCleanupReportArtifact,
        ) -> None:
            """Durably reference exact local evidence before authoritative closure."""
            cli_cleanup_evidence._verify_cleanup_evidence_lock(
                active_evidence_lock,
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
            pending = seed_report.model_copy(
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
                                "resource_id": f"{session_id}:{generation_id}",
                                "action": "close",
                                "outcome": "pending",
                                "verified_after_operation": False,
                                "residual": True,
                            },
                        ],
                        remaining_resources=[
                            ValidationResource(
                                kind="owner_session",
                                resource_id=f"{session_id}:{generation_id}",
                                role="cleanup_closure",
                                cluster=cluster,
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
                    cluster=cluster,
                    state="verified",
                    references=[str(local_artifact.manifest_path.resolve())],
                    metadata=report_metadata,
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="owner_session",
                    resource_id=f"{session_id}:{generation_id}",
                    role="cleanup_closure",
                    cluster=cluster,
                    state="pending",
                    metadata={"cleanup_operation_id": operation_id},
                )
            )
            validation_report_module.write_validation_report(pending, canonical_report_path)
            cli_cleanup_evidence._verify_cleanup_evidence_lock(
                active_evidence_lock,
                expected_parent=cli_cleanup_evidence._cleanup_evidence_state_parent(),
            )
            reread = load_validation_report(canonical_report_path)
            cli_cleanup_evidence._verify_cleanup_evidence_lock(
                active_evidence_lock,
                expected_parent=cli_cleanup_evidence._cleanup_evidence_state_parent(),
            )
            expected_checkpoint = LiveValidationReport.model_validate(
                redact_sensitive_values(pending.model_dump(mode="json"))
            )
            if reread.model_dump(mode="json") != expected_checkpoint.model_dump(mode="json"):
                raise RelayError(
                    "cleanup report artifact checkpoint changed during durable re-read"
                )
            canonical_report[0] = reread

        def emit_completed_report(
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
                    resource_id=f"{session_id}:{generation_id}",
                    location=definition.ssh_host if remote_execution else str(queue.root),
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
                stop_worker=stop_worker
            ).model_dump(mode="json")
            payload["relay_jobs"] = {
                "cancel_requested": cancel_jobs,
                "scheduler_cancel_requested": cancel_jobs and cancel_scheduler_jobs,
                "canceled_job_ids": canceled_job_ids,
            }
            payload["gateway_sessions"] = gateway_reports
            if legacy_recovery is not None:
                payload["recovery_evidence"] = legacy_recovery.model_dump(mode="json")
            payload.update(
                {
                    "validation_report": str(canonical_report_path.resolve()),
                    "validation_status": ValidationStatus.PASSED.value,
                    "validation_provenance_warning": False,
                }
            )
            preliminary = cli_cleanup_report._bounded_cleanup_public_json(payload)
            if preliminary is not None:
                canonical = projection.to_live_validation_report(
                    stop_worker=stop_worker,
                    cancel_jobs=cancel_jobs,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                ).model_copy(
                    update={
                        "report_id": seed_report.report_id,
                        "started_at": seed_report.started_at,
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
                        cluster=cluster,
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
                    canonical_report[0] = canonical
                    provenance_warning = cli_remote_worker_attach._write_cleanup_validation_report(
                        canonical,
                        definition,
                        canonical_report_path,
                        observed_worker_info=cleanup_worker_info,
                        worker_observation_error=cleanup_worker_error,
                    )
                    payload["validation_status"] = canonical.status.value
                    payload["validation_provenance_warning"] = provenance_warning
                    serialized = cli_cleanup_report._bounded_cleanup_public_json(payload)
                    if (
                        serialized is not None
                        and canonical_report_path.stat().st_size
                        < MAX_CLEANUP_VALIDATION_REPORT_BYTES
                    ):
                        typer.echo(serialized)
                        canonical_ok = canonical.status is ValidationStatus.PASSED
                        if payload.get("ok") is not True or (
                            not canonical_ok and not provenance_warning
                        ):
                            raise typer.Exit(code=1)
                        return
            emit_finalized_retry_report(
                report,
                recovery=recovery,
                local_artifact=local_artifact,
                retry=False,
            )

        def emit_finalized_retry_report(
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
                "by_action": dict(
                    sorted(Counter(item.action for item in report.resources).items())
                ),
                "by_outcome": dict(
                    sorted(Counter(item.outcome for item in report.resources).items())
                ),
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
            canonical = seed_report.model_copy(
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
                                "resource_id": f"{session_id}:{generation_id}",
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
                    cluster=cluster,
                    state="verified",
                    references=[str(local_artifact.manifest_path.resolve())],
                    metadata=report_metadata,
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="owner_session",
                    resource_id=f"{session_id}:{generation_id}",
                    role="cleanup_closure",
                    cluster=cluster,
                    state="closed",
                    metadata={
                        "cleanup_operation_id": operation_id,
                        "coordinator_report_sha256": reference.sha256,
                    },
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="owner_session_recovery",
                    resource_id=f"{session_id}:{generation_id}",
                    role="post_cleanup_recovery",
                    cluster=cluster,
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
            canonical_report[0] = canonical
            provenance_warning = cli_remote_worker_attach._write_cleanup_validation_report(
                canonical,
                definition,
                canonical_report_path,
                observed_worker_info=cleanup_worker_info,
                worker_observation_error=cleanup_worker_error,
            )
            if canonical_report_path.stat().st_size >= MAX_CLEANUP_VALIDATION_REPORT_BYTES:
                raise RelayError("finalized cleanup validation report exceeded its byte limit")
            payload: dict[str, object] = {
                "schema_version": (
                    "clio-relay.finalized-cleanup-retry.v1"
                    if retry
                    else "clio-relay.finalized-cleanup.v1"
                ),
                "cluster": cluster,
                "session_id": session_id,
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
                    "chunk_size_limit": (
                        cli_owned_report_artifact.MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES
                    ),
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
                "validation_report": str(canonical_report_path.resolve()),
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

        initial_status_error: str | None = None
        try:
            pre_teardown_status = session_lifecycle.status_remote_session(
                definition=definition,
                session_id=session_id,
            )
        except (JSONDecodeError, RelayError) as exc:
            initial_status_error = f"{type(exc).__name__}: {exc}"
            pre_teardown_status = {}
        recovery_status: OwnedSessionRecoveryStatus | None = None
        recovery_resource: ValidationResource | None = None
        try:
            session_generation_id = cli_owned_session_recovery._verified_owner_session_generation(
                pre_teardown_status,
                session_id=session_id,
            )
        except RelayError:
            session_generation_id = ""
        if not session_generation_id or pre_teardown_status.get("running") is not True:
            recovery_status = cli_owned_session_recovery._owned_session_recovery_status(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                cluster=cluster,
                session_id=session_id,
            )
            recovery_resource = (
                cli_owned_session_recovery._owner_session_recovery_validation_resource(
                    recovery_status
                )
            )
            if initial_status_error is not None:
                recovery_resource.metadata["initial_status_error"] = initial_status_error
            seed_report.resources.append(recovery_resource)
            canonical_report[0] = seed_report
            validation_report_module.write_validation_report(seed_report, canonical_report_path)
            session_generation_id = (
                cli_owned_session_recovery._verified_recovered_owner_session_generation(
                    recovery_status,
                    cluster=cluster,
                    session_id=session_id,
                )
            )
            pre_teardown_status = {
                "owner": recovery_status.owner,
                "session_id": recovery_status.session_id,
                "session_generation_id": recovery_status.session_generation_id,
                "api_pid": recovery_status.api_pid,
                "process_start_ticks": recovery_status.process_start_marker,
                "running": recovery_status.running,
                "ownership_verified": recovery_status.ownership_verified,
                "process_absence_verified": recovery_status.process_absence_verified,
                "process_state": recovery_status.process_state,
            }
        requested_policy = {
            "stop_worker": stop_worker,
            "cancel_jobs": cancel_jobs,
            "cancel_scheduler_jobs": cancel_scheduler_jobs,
        }
        finalized_retry_report: SessionLifecycleReport | None = None
        finalized_retry_reference = None
        if (
            recovery_status is not None
            and recovery_status.cleanup_receipt
            and recovery_status.coordinator_report_bound
        ):
            retrieved_report = session_lifecycle.read_remote_session_cleanup_report(
                definition=definition,
                cluster=cluster,
                session_id=session_id,
                status=recovery_status,
            )
            finalized_retry_report = cli_cleanup_report._verified_finalized_cleanup_report(
                recovery_status,
                report=retrieved_report,
                cluster=cluster,
                session_id=session_id,
                expected_generation_id=session_generation_id,
                expected_cleanup_policy=requested_policy,
            )
            finalized_retry_reference = recovery_status.coordinator_report_ref
        local_admission_session_id = _desktop_owner_session_admission_id(
            cluster=cluster,
            session_id=session_id,
        )
        if remote_execution:
            _assert_no_unscoped_desktop_admission_state(
                queue,
                cluster=cluster,
                session_id=session_id,
                session_generation_id=session_generation_id,
            )
        authoritative_admission = cli_owned_relay_jobs._owner_session_admission_status(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            session_id=session_id,
            session_generation_id=session_generation_id,
        )
        local_cleanup_intent = queue.get_owner_session_cleanup_intent(
            local_admission_session_id,
            session_generation_id=session_generation_id,
        )
        cleanup_operation_id = cli_owned_relay_jobs._select_owner_session_cleanup_operation(
            authoritative_status=authoritative_admission,
            local_intent=local_cleanup_intent,
            session_id=session_id,
            session_generation_id=session_generation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
        )
        if finalized_retry_report is not None:
            if recovery_status is None or finalized_retry_reference is None:
                raise RelayError("finalized cleanup retry lost its exact report reference")
            finalized_retry_report = cli_cleanup_report._verified_finalized_cleanup_report(
                recovery_status,
                report=finalized_retry_report,
                cluster=cluster,
                session_id=session_id,
                expected_generation_id=session_generation_id,
                expected_cleanup_operation_id=cleanup_operation_id,
                expected_cleanup_policy=requested_policy,
            )
            local_cleanup_artifact = (
                cli_owned_report_artifact._persist_local_cleanup_report_artifact(
                    finalized_retry_report,
                    validation_report_path=canonical_report_path,
                    evidence_lock=active_evidence_lock,
                )
            )
            checkpoint_finalized_cleanup_artifact(
                finalized_retry_report,
                recovery=recovery_status,
                local_artifact=local_cleanup_artifact,
            )
            cli_cleanup_evidence._verify_cleanup_evidence_lock(
                active_evidence_lock,
                expected_parent=cli_cleanup_evidence._cleanup_evidence_state_parent(),
            )
            cli_owned_session_recovery._mark_owner_session_closed(
                queue=queue,
                definition=definition,
                cluster=cluster,
                remote_execution=remote_execution,
                session_id=session_id,
                local_admission_session_id=local_admission_session_id,
                session_generation_id=session_generation_id,
                legacy_unversioned_job_ids=[],
                finalized_recovery=recovery_status,
                finalized_report=finalized_retry_report,
            )
            closed_recovery = cli_owned_session_recovery._owned_session_recovery_status(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                cluster=cluster,
                session_id=session_id,
            )
            if not (
                closed_recovery.recovery_verified
                and closed_recovery.cleanup_receipt
                and closed_recovery.cleanup_paths_pending is False
                and closed_recovery.coordinator_report_bound
                and closed_recovery.session_generation_id == session_generation_id
                and closed_recovery.process_state == "already_closed"
                and isinstance(closed_recovery.admission_status, dict)
                and closed_recovery.admission_status.get("closed") is True
            ):
                raise RelayError(
                    "finalized cleanup retry was not authoritatively closed after commit"
                )
            if closed_recovery.coordinator_report_ref != finalized_retry_reference:
                raise RelayError("finalized cleanup report reference changed during closure")
            closed_report = cli_cleanup_report._verified_finalized_cleanup_report(
                closed_recovery,
                report=finalized_retry_report,
                cluster=cluster,
                session_id=session_id,
                expected_generation_id=session_generation_id,
                expected_cleanup_operation_id=cleanup_operation_id,
                expected_cleanup_policy=requested_policy,
            )
            if session_lifecycle_report_sha256(closed_report) != session_lifecycle_report_sha256(
                finalized_retry_report
            ):
                raise RelayError("finalized cleanup report reference changed during closure")
            recovery_status = closed_recovery
            recovery_resource = (
                cli_owned_session_recovery._owner_session_recovery_validation_resource(
                    closed_recovery
                )
            )
            emit_finalized_retry_report(
                finalized_retry_report,
                recovery=recovery_status,
                local_artifact=local_cleanup_artifact,
            )
            return
        partial = seed_report
        partial.cleanup = CleanupEvidence(
            requested=True,
            mode="teardown",
            operation_id=cleanup_operation_id,
            cancel_relay_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_jobs and cancel_scheduler_jobs,
            stop_worker=stop_worker,
            actions=[
                {
                    "kind": "owner_session_admission",
                    "resource_id": f"{session_id}:{session_generation_id}",
                    "action": "quiesce",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                },
                {
                    "kind": "remote_relay_api",
                    "resource_id": session_id,
                    "action": "stop",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                },
            ],
        )
        admission_resource = ValidationResource(
            kind="owner_session_admission",
            resource_id=f"{session_id}:{session_generation_id}",
            role="cleanup_admission",
            cluster=cluster,
            state="pending",
            metadata={
                "operation_id": cleanup_operation_id,
                "local_admission_session_id": local_admission_session_id,
                "remote_execution": remote_execution,
            },
        )
        api_resource = ValidationResource(
            kind="remote_relay_api",
            resource_id=session_id,
            role="cleanup_target",
            cluster=cluster,
            state="running" if pre_teardown_status.get("running") is True else "stopped",
            metadata={
                "session_generation_id": session_generation_id,
                "ownership_verified": pre_teardown_status.get("ownership_verified") is True,
                "cleanup_operation_id": cleanup_operation_id,
            },
        )
        admission_resource_index = len(partial.resources)
        partial.resources.extend([admission_resource, api_resource])
        partial.cleanup.remaining_resources.extend([admission_resource, api_resource])
        canonical_report[0] = partial
        validation_report_module.write_validation_report(partial, canonical_report_path)
        cleanup_intent = cli_owned_relay_jobs._quiesce_owner_session_intake(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            session_id=session_id,
            local_admission_session_id=local_admission_session_id,
            session_generation_id=session_generation_id,
            cleanup_operation_id=cleanup_operation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
        )
        partial.resources[admission_resource_index] = partial.resources[
            admission_resource_index
        ].model_copy(update={"state": "quiesced"})
        partial.cleanup.remaining_resources[0] = partial.resources[admission_resource_index]
        partial.cleanup.actions[0].update(
            {
                "outcome": "quiesced",
                "verified_after_operation": True,
            }
        )
        validation_report_module.write_validation_report(partial, canonical_report_path)

        def list_owned_jobs(
            *, include_terminal: bool = False
        ) -> list[cli_owned_relay_jobs._OwnedRelayJob]:
            if remote_execution:
                return cli_owned_relay_jobs._list_remote_owned_active_cluster_jobs(
                    definition,
                    cluster,
                    owner_session_id=session_id,
                    owner_session_generation_id=session_generation_id,
                    include_terminal=include_terminal,
                )
            return cli_owned_relay_jobs._list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                scheduler_provider=definition.scheduler_provider,
                include_terminal=include_terminal,
            )

        def list_legacy_jobs() -> list[cli_owned_relay_jobs._OwnedRelayJob]:
            """Discover unversioned records without treating them as this generation's jobs."""
            if remote_execution:
                return cli_owned_relay_jobs._list_remote_owned_active_cluster_jobs(
                    definition,
                    cluster,
                    owner_session_id=session_id,
                    owner_session_generation_id=None,
                    include_terminal=True,
                )
            return cli_owned_relay_jobs._list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=None,
                scheduler_provider=definition.scheduler_provider,
                include_terminal=True,
            )

        def read_owned_job(job_id: str) -> cli_owned_relay_jobs._OwnedRelayJob:
            return cli_owned_relay_jobs._read_owned_relay_job(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                cluster=cluster,
                job_id=job_id,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )

        legacy_jobs = list_legacy_jobs()
        if legacy_jobs:
            for legacy_job in legacy_jobs:
                resource = ValidationResource(
                    kind="relay_job",
                    resource_id=legacy_job.job_id,
                    role="ambiguous_legacy_owner_session",
                    cluster=cluster,
                    state=legacy_job.relay_state.value,
                    provider=legacy_job.scheduler_provider,
                    metadata={
                        "ownership_verified": False,
                        "expected_owner_session_generation_id": session_generation_id,
                        "observed_owner_session_generation_id": None,
                        "mutation_refused": True,
                    },
                )
                partial.resources.append(resource)
                partial.cleanup.remaining_resources.append(resource)
            validation_report_module.write_validation_report(partial, canonical_report_path)
            raise RelayError(
                "owner-session cleanup found unversioned legacy jobs whose generation cannot be "
                "proven; no relay or scheduler cancellation was attempted: "
                + ", ".join(sorted(job.job_id for job in legacy_jobs))
            )

        owned_jobs = list_owned_jobs()
        if cancel_jobs:
            for job in owned_jobs:
                resource = ValidationResource(
                    kind="relay_job",
                    resource_id=job.job_id,
                    role="cleanup_cancel_target",
                    cluster=cluster,
                    state=job.relay_state.value,
                    provider=job.scheduler_provider,
                    metadata={
                        "action": "cancel",
                        "ownership_verified": True,
                        "owner_session_generation_id": session_generation_id,
                        "cleanup_operation_id": cleanup_operation_id,
                    },
                )
                partial.resources.append(resource)
                partial.cleanup.remaining_resources.append(resource)
                partial.cleanup.actions.append(
                    {
                        "kind": "relay_job",
                        "resource_id": job.job_id,
                        "action": "cancel",
                        "outcome": "pending",
                        "verified_after_operation": False,
                        "residual": True,
                    }
                )
            validation_report_module.write_validation_report(partial, canonical_report_path)
        gateway_scheduler_job_ids = (
            cli_owned_scheduler_cancel._owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
            if scheduler_sentinel_ids
            else ()
        )
        for scheduler_job_id in gateway_scheduler_job_ids:
            scheduler_resource = ValidationResource(
                kind="scheduler_job",
                resource_id=scheduler_job_id,
                role="gateway_cleanup_target",
                cluster=cluster,
                state="discovered",
                provider=definition.scheduler_provider,
                metadata={
                    "action": "cancel" if cancel_scheduler_jobs else "retain",
                    "ownership_verified": True,
                    "owner_session_generation_id": session_generation_id,
                    "cleanup_operation_id": cleanup_operation_id,
                },
            )
            partial.resources.append(scheduler_resource)
            partial.cleanup.remaining_resources.append(scheduler_resource)
            partial.cleanup.actions.append(
                {
                    "kind": "scheduler_job",
                    "resource_id": scheduler_job_id,
                    "action": "cancel" if cancel_scheduler_jobs else "retain",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                    "source": "gateway",
                }
            )
        if gateway_scheduler_job_ids:
            validation_report_module.write_validation_report(partial, canonical_report_path)
        scheduler_sentinel_pre_phases = cli_owned_scheduler_cancel._preflight_scheduler_sentinels(
            definition,
            scheduler_sentinel_ids,
            owned_jobs,
            gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        )
        canceled: list[str] = []
        if cancel_jobs:
            try:
                cancellation_targets = (
                    cli_owned_relay_jobs._cancel_remote_owned_jobs(definition, cluster, owned_jobs)
                    if remote_execution
                    else cli_owned_relay_jobs._cancel_local_owned_jobs(queue, owned_jobs)
                )
                canceled.extend(
                    cli_owned_relay_jobs._wait_for_owned_relay_cancellations(
                        cancellation_targets,
                        read_owned_job=read_owned_job,
                        timeout_seconds=relay_cancel_timeout_seconds,
                        poll_seconds=relay_cancel_poll_seconds,
                    )
                )
            except BaseException as exc:
                for action_evidence in partial.cleanup.actions:
                    if action_evidence.get("kind") == "relay_job":
                        action_evidence.update(
                            {
                                "outcome": "failed",
                                "verified_after_operation": False,
                                "residual": True,
                                "detail": str(exc),
                            }
                        )
                validation_report_module.write_validation_report(partial, canonical_report_path)
                raise
            canceled_ids = set(canceled)
            for index, resource in enumerate(partial.resources):
                if resource.kind == "relay_job" and resource.resource_id in canceled_ids:
                    partial.resources[index] = resource.model_copy(update={"state": "canceled"})
            partial.cleanup.remaining_resources = [
                resource
                for resource in partial.cleanup.remaining_resources
                if not (resource.kind == "relay_job" and resource.resource_id in canceled_ids)
            ]
            for action_evidence in partial.cleanup.actions:
                if (
                    action_evidence.get("kind") == "relay_job"
                    and action_evidence.get("resource_id") in canceled_ids
                ):
                    action_evidence.update(
                        {
                            "outcome": "canceled",
                            "verified_after_operation": True,
                            "residual": False,
                        }
                    )
            validation_report_module.write_validation_report(partial, canonical_report_path)
        gateway_reports = cli_owned_runtime_cleanup._cleanup_owned_runtime_sessions(
            cluster=cluster,
            definition=definition,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
            mode="teardown",
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            scheduler_sentinel_ids=scheduler_sentinel_ids,
            owned_jobs=owned_jobs,
        )
        report = session_lifecycle.teardown_remote_session(
            definition=definition,
            session_id=session_id,
            expected_session_generation_id=session_generation_id,
            expected_cleanup_operation_id=cast(str, cleanup_intent["operation_id"]),
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            cluster=cluster,
        )
        report.cleanup_operation_id = cast(str, cleanup_intent["operation_id"])
        report.cleanup_policy = {
            key: cast(bool, cleanup_intent[key])
            for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
        }
        report.relay_cancel_requested = cancel_jobs
        report.scheduler_cancel_requested = cancel_jobs and cancel_scheduler_jobs
        partial = report.to_live_validation_report(
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        partial = partial.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        if recovery_resource is not None:
            partial.resources.append(recovery_resource)
        canonical_report[0] = partial
        post_api_jobs = list_owned_jobs(include_terminal=True)
        initial_job_ids = {job.job_id for job in owned_jobs}
        late_jobs = [job for job in post_api_jobs if job.job_id not in initial_job_ids]
        if cancel_jobs and late_jobs:
            late_targets = (
                cli_owned_relay_jobs._cancel_remote_owned_jobs(definition, cluster, late_jobs)
                if remote_execution
                else cli_owned_relay_jobs._cancel_local_owned_jobs(queue, late_jobs)
            )
            canceled.extend(
                cli_owned_relay_jobs._wait_for_owned_relay_cancellations(
                    late_targets,
                    read_owned_job=read_owned_job,
                    timeout_seconds=relay_cancel_timeout_seconds,
                    poll_seconds=relay_cancel_poll_seconds,
                )
            )
            owned_jobs.extend(late_jobs)

        gateway_scheduler_job_ids = (
            cli_owned_scheduler_cancel._owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
            if scheduler_sentinel_ids
            else ()
        )
        cli_owned_scheduler_cancel._assert_scheduler_sentinels_unrelated(
            scheduler_sentinel_ids,
            owned_jobs,
            gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        )

        scheduler_jobs = list_owned_jobs(include_terminal=True)
        by_job_id: dict[str, cli_owned_relay_jobs._OwnedRelayJob] = {}
        for job in [*owned_jobs, *scheduler_jobs]:
            by_job_id.setdefault(job.job_id, job)
        owned_jobs = list(by_job_id.values())
        gateway_scheduler_job_ids = (
            cli_owned_scheduler_cancel._owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
            if scheduler_sentinel_ids
            else ()
        )
        cli_owned_scheduler_cancel._assert_scheduler_sentinels_unrelated(
            scheduler_sentinel_ids,
            owned_jobs,
            gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        )
        report.resources.extend(
            cli_owned_scheduler_cancel._owned_job_cleanup_resources(
                owned_jobs,
                definition=definition,
                location=definition.ssh_host,
                cancel_jobs=cancel_jobs,
                cancel_scheduler_jobs=cancel_scheduler_jobs,
                post_operation_jobs=scheduler_jobs,
            )
        )
        if cancel_jobs and cancel_scheduler_jobs:
            scheduler_resources, scheduler_errors = (
                cli_owned_scheduler_cancel._cancel_owned_scheduler_jobs(
                    definition,
                    owned_jobs,
                )
            )
            report.resources.extend(scheduler_resources)
            report.errors.extend(scheduler_errors)
        sentinel_resources, sentinel_errors = (
            cli_owned_scheduler_cancel._scheduler_sentinel_preservation_resources(
                definition,
                scheduler_sentinel_pre_phases,
            )
        )
        report.resources.extend(sentinel_resources)
        report.errors.extend(sentinel_errors)
        final_jobs = list_owned_jobs(include_terminal=True)
        if cancel_jobs:
            uncanceled = [
                job.job_id
                for job in final_jobs
                if job.relay_state in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}
                or (
                    job.relay_state is JobState.CANCELED and not job.relay_cancellation_acknowledged
                )
            ]
            if uncanceled:
                report.errors.append(
                    "owned relay jobs remained active after final rescan: "
                    + ", ".join(sorted(uncanceled))
                )
        cli_owned_runtime_cleanup._merge_gateway_cleanup_resources(report, gateway_reports)
        cli_owner_session_teardown_verify._verify_owner_session_teardown(
            report,
            session_id=session_id,
            session_generation_id=session_generation_id,
            stop_worker=stop_worker,
        )
        report, finalized_recovery = _persist_verified_cleanup_report_before_closure(
            definition=definition,
            cluster=cluster,
            session_id=session_id,
            session_generation_id=session_generation_id,
            report=report,
        )
        finalized_reference = finalized_recovery.coordinator_report_ref
        if finalized_reference is None:
            raise RelayError("finalized cleanup omitted its exact report reference")
        local_cleanup_artifact = cli_owned_report_artifact._persist_local_cleanup_report_artifact(
            report,
            validation_report_path=canonical_report_path,
            evidence_lock=active_evidence_lock,
        )
        checkpoint_finalized_cleanup_artifact(
            report,
            recovery=finalized_recovery,
            local_artifact=local_cleanup_artifact,
        )
        cli_cleanup_evidence._verify_cleanup_evidence_lock(
            active_evidence_lock,
            expected_parent=cli_cleanup_evidence._cleanup_evidence_state_parent(),
        )
        legacy_recovery = recovery_status
        legacy_unversioned_job_ids: list[str] = []
        cli_owned_session_recovery._mark_owner_session_closed(
            queue=queue,
            definition=definition,
            cluster=cluster,
            remote_execution=remote_execution,
            session_id=session_id,
            local_admission_session_id=local_admission_session_id,
            session_generation_id=session_generation_id,
            legacy_unversioned_job_ids=legacy_unversioned_job_ids,
            finalized_recovery=finalized_recovery,
            finalized_report=report,
        )
        closed_recovery = cli_owned_session_recovery._owned_session_recovery_status(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            cluster=cluster,
            session_id=session_id,
        )
        if not (
            closed_recovery.recovery_verified
            and closed_recovery.cleanup_receipt
            and closed_recovery.cleanup_paths_pending is False
            and closed_recovery.coordinator_report_bound
            and closed_recovery.session_generation_id == session_generation_id
            and closed_recovery.process_state == "already_closed"
            and isinstance(closed_recovery.admission_status, dict)
            and closed_recovery.admission_status.get("closed") is True
            and closed_recovery.coordinator_report_ref == finalized_reference
        ):
            raise RelayError("cleanup was not authoritatively closed after commit")
        closed_report = cli_cleanup_report._verified_finalized_cleanup_report(
            closed_recovery,
            report=report,
            cluster=cluster,
            session_id=session_id,
            expected_generation_id=session_generation_id,
            expected_cleanup_operation_id=cleanup_operation_id,
            expected_cleanup_policy=requested_policy,
        )
        if session_lifecycle_report_sha256(closed_report) != local_cleanup_artifact.report_sha256:
            raise RelayError("finalized cleanup report changed during authoritative closure")
        recovery_status = closed_recovery
        recovery_resource = cli_owned_session_recovery._owner_session_recovery_validation_resource(
            closed_recovery
        )
        emit_completed_report(
            report,
            canceled_job_ids=canceled,
            gateway_reports=gateway_reports,
            recovery=closed_recovery,
            local_artifact=local_cleanup_artifact,
            legacy_recovery=legacy_recovery,
        )

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            cli._write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="cleanup",
                cluster=cluster,
                check_id="session.teardown",
                summary="teardown owned desktop session resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    def locked_action() -> None:
        with (
            remote_cli.remote_command_timeout(
                cli_owned_relay_jobs.REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS
            ),
            cli._session_transition_lock(cluster=cluster, session_id=session_id),
        ):
            guarded_action()

    try:
        cli._run_or_exit(locked_action)
    finally:
        cli_cleanup_evidence._release_cleanup_evidence_lock(evidence_lock)

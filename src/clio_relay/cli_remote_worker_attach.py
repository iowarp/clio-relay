"""Remote worker attach/verify reporting (iowarp/clio-relay#231
continuation): the acceptance-report writers used once a remote worker
has been attached and verified."""

from __future__ import annotations

from contextlib import suppress
from pathlib import Path

import clio_relay.cli_remote_worker_probe as cli_remote_worker_probe
import clio_relay.remote_cli as remote_cli
import clio_relay.validation_report as validation_report_module
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.installation import (
    attach_verified_worker_identity,
)
from clio_relay.validation_report import (
    CleanupEvidence,
    LiveValidationReport,
    ValidationRecorder,
    new_live_validation_report,
    sha256_file,
)

REMOTE_CLEANUP_WORKER_INFO_TIMEOUT_SECONDS = 20.0


def _attach_verified_remote_worker(
    report: LiveValidationReport,
    definition: ClusterDefinition,
    *,
    observed_worker_info: dict[str, object] | None = None,
) -> None:
    """Attach exact remote installation identity when the target executes over SSH."""
    if not remote_cli.should_execute_on_cluster(definition):
        return
    remote_info = (
        observed_worker_info
        if observed_worker_info is not None
        else cli_remote_worker_probe._remote_worker_info(definition)
    )
    attach_verified_worker_identity(report, remote_info)


def _observe_worker_before_cleanup(
    definition: ClusterDefinition,
) -> tuple[dict[str, object] | None, Exception | None]:
    """Capture bounded worker evidence before cleanup can stop remote services."""
    if not remote_cli.should_execute_on_cluster(definition):
        return None, None
    try:
        return (
            cli_remote_worker_probe._remote_worker_info(
                definition,
                timeout_seconds=REMOTE_CLEANUP_WORKER_INFO_TIMEOUT_SECONDS,
            ),
            None,
        )
    except Exception as exc:
        return None, exc


def _write_remote_verified_report(
    report: LiveValidationReport,
    definition: ClusterDefinition,
    path: Path,
    *,
    observed_worker_info: dict[str, object] | None = None,
    worker_observation_error: Exception | None = None,
) -> None:
    """Persist a report only after recording remote installation verification."""
    if observed_worker_info is not None and worker_observation_error is not None:
        raise ValueError("worker observation cannot contain both info and an error")
    try:
        if worker_observation_error is not None:
            raise worker_observation_error
        _attach_verified_remote_worker(
            report,
            definition,
            observed_worker_info=observed_worker_info,
        )
        if observed_worker_info is not None:
            for resource in report.resources:
                if (
                    resource.kind == "relay_worker"
                    and resource.resource_id == f"worker:{definition.name}"
                ):
                    resource.metadata["observation_phase"] = "before_cleanup"
    except BaseException as exc:
        recorder = ValidationRecorder(report)
        recorder.record_failure(
            "worker.installation-info",
            "verify remote worker installation identity",
            exc,
        )
        recorder.finish(exc)
        recorder.write(path)
        raise
    validation_report_module.write_validation_report(report, path)


def _write_cleanup_validation_report(
    report: LiveValidationReport,
    definition: ClusterDefinition,
    path: Path,
    *,
    observed_worker_info: dict[str, object] | None = None,
    worker_observation_error: Exception | None = None,
) -> bool:
    """Persist operational cleanup without manufacturing release provenance.

    Ordinary detach and teardown commands do not require a candidate wheel.  When
    no independently computed artifact digest was supplied, the cleanup report
    remains an honest operational result with unverified artifact provenance and
    without verified-worker checks.  The release gate therefore cannot consume it
    as released-artifact evidence.  If a digest is supplied, remote worker
    verification remains strict and any mismatch still fails the acceptance run.

    A bounded pre-cleanup worker observation is optional operational metadata.  Its
    failure is recorded as failed provenance evidence, but it must not hide a
    completed cleanup receipt or change the cleanup command's operational result.
    Return ``True`` only when that optional provenance warning was recorded.
    """
    if report.install_source.artifact_sha256 is None:
        validation_report_module.write_validation_report(report, path)
        return False
    if worker_observation_error is not None:
        recorder = ValidationRecorder(report)
        recorder.record_failure(
            "worker.installation-info",
            "verify remote worker installation identity",
            worker_observation_error,
        )
        recorder.finish(worker_observation_error)
        recorder.write(path)
        return True
    _write_remote_verified_report(
        report,
        definition,
        path,
        observed_worker_info=observed_worker_info,
        worker_observation_error=worker_observation_error,
    )
    return False


def _new_cleanup_acceptance_report(
    *,
    scenario: str,
    cluster: str,
    mode: str,
    resource_kind: str,
    resource_id: str,
    action: str,
    cancel_relay_jobs: bool,
    cancel_scheduler_jobs: bool,
    stop_worker: bool,
    launcher: str | None,
    install_source: str | None,
    artifact: Path | None,
) -> LiveValidationReport:
    """Seed requested cleanup policy before any fallible preflight or observation."""
    artifact_sha256: str | None = None
    if artifact is not None:
        with suppress(OSError):
            artifact_sha256 = sha256_file(artifact)
    report = new_live_validation_report(
        scenario=scenario,
        cluster=cluster,
        launcher=launcher,
        install_source=install_source,
        artifact_sha256=artifact_sha256,
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode=mode,
        cancel_relay_jobs=cancel_relay_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
        stop_worker=stop_worker,
        actions=[
            {
                "kind": resource_kind,
                "resource_id": resource_id,
                "action": action,
                "outcome": "pending",
                "verified_after_operation": False,
                "residual": True,
            }
        ],
    )
    return report

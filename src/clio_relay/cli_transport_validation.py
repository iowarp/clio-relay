"""FRP/SSH transport validation (iowarp/clio-relay#231 continuation): the
two connection-check runners ``relay-host test-frpc-connection`` and
``relay-host test-ssh-transport`` (etc.) drive."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path

import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach
import clio_relay.frp_check as frp_check
from clio_relay.errors import RelayError
from clio_relay.relay_host import FrpcConfig
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    default_report_path,
    new_live_validation_report,
    sha256_file,
)


def _run_transport_validation(  # pyright: ignore[reportUnusedFunction]
    *,
    cluster: str,
    transport_mode: str,
    resource_id: str,
    resource_role: str,
    retain_remote_session: bool,
    validation_report: Path | None,
    validation_launcher: str | None,
    validation_install_source: str | None,
    validation_artifact: Path | None,
    probe: Callable[[], list[str]],
) -> list[str]:
    """Run one transport probe and persist canonical success or failure evidence."""
    import clio_relay.cli as cli

    report_path = validation_report or default_report_path(cluster)
    connector = ValidationResource(
        kind="connector",
        resource_id=resource_id,
        role=resource_role,
        cluster=cluster,
        state="starting",
        metadata={"transport_mode": transport_mode},
    )
    report = new_live_validation_report(
        scenario="transport",
        cluster=cluster,
        transport_modes=[transport_mode],
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode=("transport_probe_detach" if retain_remote_session else "transport_probe_teardown"),
        cancel_scheduler_jobs=False,
    )
    recorder = ValidationRecorder(report)
    try:
        lines = probe()
    except BaseException as exc:
        failed_connector = connector.model_copy(
            update={
                "state": "unknown",
                "metadata": {
                    **connector.metadata,
                    "cleanup_verified": False,
                },
            }
        )
        recorder.add_resource(failed_connector)
        recorder.report.cleanup.actions.append(
            {
                "kind": "transport_probe",
                "resource_id": resource_id,
                "action": "detach" if retain_remote_session else "teardown",
                "outcome": "failed",
            }
        )
        recorder.report.cleanup.remaining_resources.append(failed_connector)
        recorder.record_failure("transport.completed", "complete transport probe", exc)
        recorder.finish(exc)
        recorder.write(report_path)
        raise

    for line in lines:
        recorder.observe_line(line)
    expected_cleanup_line = (
        "transport.cleanup=detached" if retain_remote_session else "transport.cleanup=passed"
    )
    cleanup_verified = expected_cleanup_line in lines and (
        not retain_remote_session or "transport.remote_session=retained" in lines
    )
    if not cleanup_verified:
        expected = (
            "verified active remote-session retention"
            if retain_remote_session
            else "verified transport teardown"
        )
        error = RelayError(f"transport probe returned without {expected} evidence")
        failed_connector = connector.model_copy(
            update={
                "state": "unknown",
                "metadata": {**connector.metadata, "cleanup_verified": False},
            }
        )
        recorder.add_resource(failed_connector)
        recorder.report.cleanup.remaining_resources.append(failed_connector)
        recorder.record_failure("transport.cleanup", "verify transport cleanup", error)
        recorder.finish(error)
        recorder.write(report_path)
        raise error
    recorder.add_resource(
        connector.model_copy(
            update={
                "state": "stopped",
                "metadata": {
                    **connector.metadata,
                    "cleanup_verified": True,
                    "remote_session_retained": retain_remote_session,
                },
            }
        )
    )
    action_outcome = "detached" if retain_remote_session else "stopped"
    recorder.report.cleanup.actions.append(
        {
            "kind": "transport_probe",
            "resource_id": resource_id,
            "action": "detach" if retain_remote_session else "teardown",
            "outcome": action_outcome,
        }
    )
    if retain_remote_session:
        retained_session = ValidationResource(
            kind="relay_session",
            resource_id=resource_id,
            role="transport_probe",
            cluster=cluster,
            state="retained",
            metadata={
                "ownership": "clio-relay",
                "ownership_verified": True,
                "verified_after_operation": True,
            },
        )
        recorder.add_resource(retained_session)
        recorder.report.cleanup.actions.append(
            {
                "kind": "relay_session",
                "resource_id": resource_id,
                "action": "retain",
                "outcome": "retained",
                "ownership_verified": True,
                "verified_after_operation": True,
            }
        )
    try:
        cli_remote_worker_attach._attach_verified_remote_worker(
            recorder.report, cli._require_cluster(cluster)
        )
    except BaseException as exc:
        recorder.record_failure(
            "worker.installation-info",
            "verify remote worker installation identity",
            exc,
        )
        recorder.finish(exc)
        recorder.write(report_path)
        raise
    recorder.finish()
    recorder.write(report_path)
    lines.append(f"validation.report={report_path.resolve()}")
    return lines


def _run_frpc_connection_validation(  # pyright: ignore[reportUnusedFunction]
    *,
    cluster: str,
    proxy_name: str,
    frpc_bin: str,
    config: FrpcConfig,
    timeout_seconds: float,
    validation_report: Path,
    validation_launcher: str | None,
    validation_install_source: str | None,
    validation_artifact: Path | None,
) -> list[str]:
    """Run the bounded frpc process probe and persist canonical evidence."""
    import clio_relay.cli as cli

    report = new_live_validation_report(
        scenario="transport",
        cluster=cluster,
        transport_modes=[config.transport_protocol.value],
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode="frpc_connection_probe",
        cancel_scheduler_jobs=False,
    )
    recorder = ValidationRecorder(report)
    connector = ValidationResource(
        kind="connector",
        resource_id=proxy_name,
        role="frpc_connection_probe",
        cluster=cluster,
        state="starting",
        metadata={"transport_mode": config.transport_protocol.value},
    )

    def record_stopped_connector() -> None:
        recorder.add_resource(
            connector.model_copy(
                update={
                    "state": "stopped",
                    "metadata": {**connector.metadata, "cleanup_verified": True},
                }
            )
        )
        if not any(
            action.get("kind") == "connector" and action.get("resource_id") == proxy_name
            for action in recorder.report.cleanup.actions
        ):
            recorder.report.cleanup.actions.append(
                {
                    "kind": "connector",
                    "resource_id": proxy_name,
                    "action": "stop",
                    "outcome": "stopped",
                    "ownership_verified": True,
                    "verified_after_operation": True,
                }
            )

    try:
        with recorder.check(
            "transport.frpc-connection",
            "frpc stayed connected for the bounded probe interval",
        ) as evidence:
            lines = frp_check.run_frpc_connection_check(
                frpc_bin=frpc_bin,
                config=config,
                timeout_seconds=timeout_seconds,
            )
            output = "\n".join(lines)
            evidence.append(
                EvidenceReference(
                    kind="frpc_probe",
                    excerpt=lines[0] if lines else "frpc connection probe completed",
                    metadata={
                        "line_count": len(lines),
                        "output_sha256": hashlib.sha256(output.encode()).hexdigest(),
                        "timeout_seconds": timeout_seconds,
                    },
                )
            )
        record_stopped_connector()
        cli_remote_worker_attach._attach_verified_remote_worker(
            recorder.report, cli._require_cluster(cluster)
        )
    except BaseException as exc:
        if not recorder.report.checks:
            recorder.record_failure(
                "transport.frpc-connection",
                "frpc stayed connected for the bounded probe interval",
                exc,
            )
        elif all(check.status is ValidationStatus.PASSED for check in recorder.report.checks):
            recorder.record_failure(
                "worker.installation-info",
                "verify remote worker installation identity",
                exc,
            )
        record_stopped_connector()
        recorder.finish(exc)
        recorder.write(validation_report)
        raise
    recorder.finish()
    recorder.write(validation_report)
    lines.append(f"validation.report={validation_report.resolve()}")
    return lines

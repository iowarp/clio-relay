"""Deterministic live validation for scheduler provider lifecycle semantics."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import cast
from uuid import uuid4

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.models import SchedulerPhase, SchedulerStatus
from clio_relay.remote_cli import run_remote_clio, should_execute_on_cluster
from clio_relay.scheduler_providers import validation_provider_for_scheduler
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    new_live_validation_report,
)


def run_scheduler_lifecycle_validation(
    *,
    cluster: str,
    definition: ClusterDefinition,
    provider: str,
    run_seconds: int,
    timeout_seconds: float,
    poll_seconds: float,
    launcher: str | None = None,
    install_source: str | None = None,
    artifact_sha256: str | None = None,
) -> LiveValidationReport:
    """Exercise held, released, allocated, running, and terminal provider states."""
    if run_seconds < 5 or run_seconds > 300:
        raise ValueError("run_seconds must be between 5 and 300")
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("timeout_seconds and poll_seconds must be positive")
    report = new_live_validation_report(
        scenario="scheduler-lifecycle",
        cluster=cluster,
        launcher=launcher,
        install_source=install_source,
        artifact_sha256=artifact_sha256,
    )
    recorder = ValidationRecorder(report)
    scheduler_job_id: str | None = None
    observations: list[dict[str, object]] = []
    terminal_completed = False
    primary_error: Exception | None = None
    try:
        with recorder.check(
            "scheduler.submit-held", "submit bounded held scheduler work"
        ) as evidence:
            scheduler_job_id = _submit_held_job(
                definition,
                provider=provider,
                job_name=f"clio-relay-validation-{uuid4().hex[:12]}",
                run_seconds=run_seconds,
            )
            evidence.append(
                EvidenceReference(
                    kind="scheduler_submission",
                    excerpt=f"held scheduler job submitted: {scheduler_job_id}",
                    metadata={"scheduler_job_id": scheduler_job_id, "provider": provider},
                )
            )

        with recorder.check("scheduler.pending", "held scheduler job is pending") as evidence:
            pending = _wait_for_status(
                definition,
                scheduler_job_id,
                provider=provider,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
                predicate=lambda status: status.phase is SchedulerPhase.PENDING,
            )
            observations.append(pending.model_dump(mode="json"))
            evidence.append(_status_evidence(pending, "held job observed before release"))

        with recorder.check("scheduler.release", "release exact held scheduler job") as evidence:
            _release_held_job(definition, scheduler_job_id, provider=provider)
            evidence.append(
                EvidenceReference(
                    kind="scheduler_release",
                    excerpt=f"released scheduler job: {scheduler_job_id}",
                )
            )

        with recorder.check(
            "scheduler.allocation-proven",
            "scheduler assigned execution nodes to the validation job",
        ) as evidence:
            allocated = _wait_for_status(
                definition,
                scheduler_job_id,
                provider=provider,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
                predicate=lambda status: (
                    status.phase in {SchedulerPhase.ALLOCATED, SchedulerPhase.RUNNING}
                    and status.nodes is not None
                    and status.nodes > 0
                ),
            )
            observations.append(allocated.model_dump(mode="json"))
            evidence.append(
                _status_evidence(
                    allocated,
                    "allocation proven by provider-assigned node count",
                )
            )

        with recorder.check("scheduler.running", "validation job is running") as evidence:
            running = _wait_for_status(
                definition,
                scheduler_job_id,
                provider=provider,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
                predicate=lambda status: status.phase is SchedulerPhase.RUNNING,
            )
            observations.append(running.model_dump(mode="json"))
            evidence.append(_status_evidence(running, "fresh running observation"))

        with recorder.check("scheduler.completed", "validation job completed") as evidence:
            completed = _wait_for_status(
                definition,
                scheduler_job_id,
                provider=provider,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
                predicate=lambda status: status.phase is SchedulerPhase.COMPLETED,
            )
            observations.append(completed.model_dump(mode="json"))
            terminal_completed = True
            evidence.append(_status_evidence(completed, "provider terminal history"))

        with recorder.check(
            "scheduler.structured-metadata",
            "provider returned structured identity and phase metadata",
        ) as evidence:
            expected_phases = {
                SchedulerPhase.PENDING.value,
                SchedulerPhase.RUNNING.value,
                SchedulerPhase.COMPLETED.value,
            }
            observed_phases = {str(item.get("phase")) for item in observations}
            if not expected_phases.issubset(observed_phases):
                raise RelayError(
                    f"structured scheduler observations are incomplete: {sorted(observed_phases)}"
                )
            if any(
                item.get("scheduler") != provider
                or item.get("scheduler_job_id") != scheduler_job_id
                for item in observations
            ):
                raise RelayError("structured scheduler observations changed provider or job id")
            evidence.append(
                EvidenceReference(
                    kind="scheduler_structured_metadata",
                    excerpt=(
                        f"provider={provider}; job={scheduler_job_id}; "
                        f"phases={','.join(sorted(observed_phases))}"
                    ),
                    metadata={"observations": observations},
                )
            )
    except Exception as exc:
        primary_error = exc
    finally:
        cleanup_error: Exception | None = None
        if scheduler_job_id is not None and not terminal_completed:
            try:
                _cancel_job(definition, scheduler_job_id, provider=provider)
                canceled = _wait_for_status(
                    definition,
                    scheduler_job_id,
                    provider=provider,
                    timeout_seconds=min(timeout_seconds, 60.0),
                    poll_seconds=poll_seconds,
                    predicate=lambda status: status.phase is SchedulerPhase.CANCELED,
                )
                observations.append(canceled.model_dump(mode="json"))
                report.cleanup = CleanupEvidence(
                    requested=True,
                    mode="scheduler-validation-failure",
                    cancel_scheduler_jobs=True,
                    actions=[
                        {
                            "kind": "scheduler_job",
                            "resource_id": scheduler_job_id,
                            "action": "cancel",
                            "outcome": "canceled",
                            "provider": provider,
                        }
                    ],
                )
            except Exception as exc:
                cleanup_error = exc
                report.cleanup = CleanupEvidence(
                    requested=True,
                    mode="scheduler-validation-failure",
                    cancel_scheduler_jobs=True,
                    actions=[
                        {
                            "kind": "scheduler_job",
                            "resource_id": scheduler_job_id,
                            "action": "cancel",
                            "outcome": "failed",
                            "provider": provider,
                            "error": str(exc),
                        }
                    ],
                    remaining_resources=[
                        ValidationResource(
                            kind="scheduler_job",
                            resource_id=scheduler_job_id,
                            role="validation_cleanup_residual",
                            cluster=cluster,
                            provider=provider,
                            state="unknown",
                        )
                    ],
                )
        if scheduler_job_id is not None:
            final_state = str(observations[-1].get("phase")) if observations else "unknown"
            recorder.add_resource(
                ValidationResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    role="held_lifecycle_validation",
                    cluster=cluster,
                    state=final_state,
                    provider=provider,
                    metadata={
                        "owned_validation_job": True,
                        "observations": observations,
                    },
                )
            )
        final_error = cleanup_error or primary_error
        recorder.finish(final_error)
    return report


def _submit_held_job(
    definition: ClusterDefinition,
    *,
    provider: str,
    job_name: str,
    run_seconds: int,
) -> str:
    if should_execute_on_cluster(definition):
        payload = _remote_json(
            definition,
            [
                "scheduler",
                "submit-held-validation",
                "--cluster",
                definition.name,
                "--provider",
                provider,
                "--job-name",
                job_name,
                "--run-seconds",
                str(run_seconds),
            ],
        )
        scheduler_job_id = payload.get("scheduler_job_id")
        if not isinstance(scheduler_job_id, str):
            raise RelayError("held scheduler submission returned no job id")
        return scheduler_job_id
    return validation_provider_for_scheduler(provider).submit_held_validation_job(
        job_name=job_name,
        run_seconds=run_seconds,
    )


def _release_held_job(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    provider: str,
) -> None:
    if should_execute_on_cluster(definition):
        payload = _remote_json(
            definition,
            [
                "scheduler",
                "release-validation",
                scheduler_job_id,
                "--cluster",
                definition.name,
                "--provider",
                provider,
            ],
        )
        if payload.get("accepted") is not True:
            raise RelayError(f"scheduler release was rejected: {scheduler_job_id}")
        return
    result = validation_provider_for_scheduler(provider).release_validation_job(scheduler_job_id)
    if result.returncode != 0:
        raise RelayError(result.stderr.strip() or "scheduler release failed")


def _cancel_job(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    provider: str,
) -> None:
    if should_execute_on_cluster(definition):
        payload = _remote_json(
            definition,
            [
                "scheduler",
                "cancel",
                scheduler_job_id,
                "--cluster",
                definition.name,
                "--provider",
                provider,
            ],
        )
        if payload.get("accepted") is not True:
            raise RelayError(f"scheduler cancellation was rejected: {scheduler_job_id}")
        return
    result = validation_provider_for_scheduler(provider).cancel(scheduler_job_id)
    if result.returncode != 0:
        raise RelayError(result.stderr.strip() or "scheduler cancellation failed")


def _wait_for_status(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    provider: str,
    timeout_seconds: float,
    poll_seconds: float,
    predicate: Callable[[SchedulerStatus], bool],
) -> SchedulerStatus:
    deadline = time.monotonic() + timeout_seconds
    last_status: SchedulerStatus | None = None
    while time.monotonic() < deadline:
        last_status = _poll_status(definition, scheduler_job_id, provider=provider)
        if predicate(last_status):
            return last_status
        if last_status.phase in {
            SchedulerPhase.COMPLETED,
            SchedulerPhase.CANCELED,
            SchedulerPhase.FAILED,
        }:
            break
        time.sleep(poll_seconds)
    phase = "unobserved" if last_status is None else last_status.phase.value
    raise TimeoutError(
        f"scheduler job {scheduler_job_id} did not reach the required state; last phase={phase}"
    )


def _poll_status(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    provider: str,
) -> SchedulerStatus:
    if should_execute_on_cluster(definition):
        return SchedulerStatus.model_validate(
            _remote_json(
                definition,
                [
                    "scheduler",
                    "status",
                    scheduler_job_id,
                    "--cluster",
                    definition.name,
                    "--provider",
                    provider,
                ],
            )
        )
    return validation_provider_for_scheduler(provider).poll(scheduler_job_id)


def _remote_json(definition: ClusterDefinition, args: list[str]) -> dict[str, object]:
    value = cast(object, json.loads(run_remote_clio(definition, args)))
    if not isinstance(value, dict):
        raise RelayError("remote scheduler command did not return a JSON object")
    return {str(key): item for key, item in cast(dict[object, object], value).items()}


def _status_evidence(status: SchedulerStatus, note: str) -> EvidenceReference:
    return EvidenceReference(
        kind="scheduler_status",
        excerpt=(
            f"{status.scheduler_job_id} phase={status.phase.value} nodes={status.nodes}: {note}"
        ),
        metadata=status.model_dump(mode="json"),
    )

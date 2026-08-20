"""End-of-run cleanup and scheduler-phase waiting for queue validation.

Owns the "the live fixture must leave the cluster clean no matter how it
exited" concern: canceling/recording every job the validation run still
owns (including waiting out a residual worker process's cancellation), and
canceling/recording an unreleased scheduler fixture job -- plus the shared
scheduler-phase poll loop both the cleanup path and the main validation
flow wait on. Moved verbatim out of ``queue_validation.py``
(iowarp/clio-relay#231-style split); no behavior changed.
"""

from __future__ import annotations

import time

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.live_validation_jobs import _record_job_cleanup
from clio_relay.live_validation_process import (
    _wait_for_worker_cancellation,
    _WorkerProcessObservation,
)
from clio_relay.live_validation_support import _require
from clio_relay.models import TERMINAL_STATES, SchedulerPhase, SchedulerStatus
from clio_relay.queue_management import cancel_queue_job
from clio_relay.scheduler_providers import SchedulerValidationProvider
from clio_relay.validation_report import ValidationRecorder, ValidationResource


def _cleanup_validation_resources(
    recorder: ValidationRecorder,
    queue: ClioCoreQueue,
    *,
    cluster: str,
    owned_jobs: dict[str, str],
    process_observations: dict[str, _WorkerProcessObservation],
    scheduler_provider: SchedulerValidationProvider | None,
    scheduler_job_id: str | None,
    scheduler_terminal: bool,
    timeout_seconds: float,
    poll_seconds: float,
) -> Exception | None:
    errors: list[str] = []
    for owned_job_id, role in owned_jobs.items():
        try:
            job = queue.get_job(owned_job_id)
            initial_state = job.state
            if job.state not in TERMINAL_STATES:
                cancel_queue_job(
                    queue,
                    owned_job_id,
                    cluster=cluster,
                    scheduler_policy="relay-only",
                )
                job = queue.get_job(owned_job_id)
            observation = process_observations.get(owned_job_id)
            metadata: dict[str, object] = {}
            if observation is not None:
                metadata = {
                    **observation.as_metadata(),
                    **_wait_for_worker_cancellation(
                        queue,
                        observation,
                        timeout_seconds=timeout_seconds,
                        poll_seconds=poll_seconds,
                    ),
                }
            if not any(
                resource.kind == "relay_job" and resource.resource_id == owned_job_id
                for resource in recorder.report.resources
            ):
                _record_job_cleanup(
                    recorder,
                    job,
                    role=role,
                    initial_state=initial_state,
                    action="cancel_validation_residual",
                    task_id=None if observation is None else observation.task_id,
                    metadata=metadata,
                )
        except Exception as exc:
            errors.append(f"relay job {owned_job_id}: {exc}")
            recorder.report.cleanup.remaining_resources.append(
                ValidationResource(
                    kind="relay_job",
                    resource_id=owned_job_id,
                    role=role,
                    cluster=cluster,
                    state="process_residual" if owned_job_id in process_observations else "unknown",
                )
            )
    if scheduler_job_id is not None and not scheduler_terminal:
        if scheduler_provider is None:
            errors.append(f"scheduler job {scheduler_job_id}: provider unavailable")
        else:
            try:
                canceled = scheduler_provider.cancel(scheduler_job_id)
                _require(
                    canceled.returncode == 0,
                    canceled.stderr.strip() or "scheduler fixture cancellation failed",
                )
                terminal = _wait_for_scheduler_phase(
                    scheduler_provider,
                    scheduler_job_id,
                    required={SchedulerPhase.CANCELED, SchedulerPhase.COMPLETED},
                    timeout_seconds=min(60.0, timeout_seconds),
                    poll_seconds=poll_seconds,
                )
                recorder.report.cleanup.cancel_scheduler_jobs = True
                recorder.report.cleanup.actions.append(
                    {
                        "kind": "scheduler_job",
                        "resource_id": scheduler_job_id,
                        "action": "cancel_failure_fixture",
                        "outcome": terminal.phase.value,
                        "provider": scheduler_provider.name,
                    }
                )
            except Exception as exc:
                errors.append(f"scheduler job {scheduler_job_id}: {exc}")
                recorder.report.cleanup.cancel_scheduler_jobs = True
                recorder.report.cleanup.remaining_resources.append(
                    ValidationResource(
                        kind="scheduler_job",
                        resource_id=scheduler_job_id,
                        role="validation_cleanup_residual",
                        cluster=cluster,
                        state="unknown",
                        provider=scheduler_provider.name,
                    )
                )
    return (
        None if not errors else RelayError("queue validation cleanup failed: " + "; ".join(errors))
    )


def _wait_for_scheduler_phase(
    provider: SchedulerValidationProvider,
    scheduler_job_id: str,
    *,
    required: set[SchedulerPhase],
    timeout_seconds: float,
    poll_seconds: float,
) -> SchedulerStatus:
    deadline = time.monotonic() + timeout_seconds
    last_status: SchedulerStatus | None = None
    while time.monotonic() < deadline:
        last_status = provider.poll(scheduler_job_id)
        _require(
            last_status.scheduler_job_id == scheduler_job_id,
            "scheduler provider returned another job identity",
        )
        _require(
            last_status.scheduler == provider.name,
            "scheduler provider returned another provider identity",
        )
        if last_status.phase in required:
            return last_status
        if last_status.phase in {
            SchedulerPhase.COMPLETED,
            SchedulerPhase.CANCELED,
            SchedulerPhase.FAILED,
        }:
            break
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
    last_phase = "unobserved" if last_status is None else last_status.phase.value
    expected = ",".join(sorted(phase.value for phase in required))
    raise TimeoutError(
        f"scheduler job {scheduler_job_id} did not reach {expected}; last phase={last_phase}"
    )

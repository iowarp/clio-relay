from __future__ import annotations

import subprocess
from typing import cast

from pytest import MonkeyPatch

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.models import SchedulerPhase, SchedulerStatus
from clio_relay.scheduler_validation import run_scheduler_lifecycle_validation
from clio_relay.validation_report import ValidationStatus


class DeterministicValidationProvider:
    """In-memory provider used to exercise orchestration state transitions."""

    name = "slurm"

    def __init__(self, statuses: list[SchedulerStatus]) -> None:
        self.statuses = statuses
        self.submitted: list[tuple[str, int]] = []
        self.released: list[str] = []
        self.canceled: list[str] = []

    def submit_held_validation_job(self, *, job_name: str, run_seconds: int) -> str:
        self.submitted.append((job_name, run_seconds))
        return "validation-123"

    def release_validation_job(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        self.released.append(scheduler_job_id)
        return subprocess.CompletedProcess(["release", scheduler_job_id], 0, "", "")

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        status = self.statuses.pop(0)
        return status.model_copy(update={"scheduler_job_id": scheduler_job_id})

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        self.canceled.append(scheduler_job_id)
        return subprocess.CompletedProcess(["cancel", scheduler_job_id], 0, "", "")


def _status(phase: SchedulerPhase, *, nodes: int | None = None) -> SchedulerStatus:
    return SchedulerStatus(
        scheduler="slurm",
        scheduler_job_id="validation-123",
        phase=phase,
        nodes=nodes,
    )


def test_scheduler_lifecycle_validation_observes_real_ordered_states(
    monkeypatch: MonkeyPatch,
) -> None:
    provider = DeterministicValidationProvider(
        [
            _status(SchedulerPhase.PENDING),
            _status(SchedulerPhase.RUNNING, nodes=1),
            _status(SchedulerPhase.RUNNING, nodes=1),
            _status(SchedulerPhase.COMPLETED, nodes=1),
        ]
    )
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")

    def resolve_provider(_name: str | None) -> DeterministicValidationProvider:
        return provider

    monkeypatch.setattr(
        "clio_relay.scheduler_validation.validation_provider_for_scheduler",
        resolve_provider,
    )

    report = run_scheduler_lifecycle_validation(
        cluster="ares",
        definition=ClusterDefinition(
            name="ares",
            ssh_host="localhost",
            scheduler_provider="slurm",
        ),
        provider="slurm",
        run_seconds=30,
        timeout_seconds=5,
        poll_seconds=0.001,
    )

    assert report.status is ValidationStatus.PASSED
    assert [check.check_id for check in report.checks] == [
        "scheduler.submit-held",
        "scheduler.pending",
        "scheduler.release",
        "scheduler.allocation-proven",
        "scheduler.running",
        "scheduler.completed",
        "scheduler.structured-metadata",
    ]
    assert provider.released == ["validation-123"]
    assert provider.canceled == []
    assert report.resources[0].resource_id == "validation-123"
    observations = report.resources[0].metadata["observations"]
    assert isinstance(observations, list)
    typed_observations = [
        cast(dict[str, object], item) for item in cast(list[object], observations)
    ]
    assert [item["phase"] for item in typed_observations] == [
        "pending",
        "running",
        "running",
        "completed",
    ]


def test_scheduler_lifecycle_validation_cancels_exact_job_on_failure(
    monkeypatch: MonkeyPatch,
) -> None:
    provider = DeterministicValidationProvider(
        [
            _status(SchedulerPhase.PENDING),
            _status(SchedulerPhase.FAILED),
            _status(SchedulerPhase.CANCELED),
        ]
    )
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")

    def resolve_provider(_name: str | None) -> DeterministicValidationProvider:
        return provider

    monkeypatch.setattr(
        "clio_relay.scheduler_validation.validation_provider_for_scheduler",
        resolve_provider,
    )

    report = run_scheduler_lifecycle_validation(
        cluster="ares",
        definition=ClusterDefinition(
            name="ares",
            ssh_host="localhost",
            scheduler_provider="slurm",
        ),
        provider="slurm",
        run_seconds=30,
        timeout_seconds=5,
        poll_seconds=0.001,
    )

    assert report.status is ValidationStatus.FAILED
    assert provider.canceled == ["validation-123"]
    assert report.cleanup.cancel_scheduler_jobs is True
    assert report.cleanup.actions[0]["resource_id"] == "validation-123"
    assert report.cleanup.actions[0]["outcome"] == "canceled"
    assert report.cleanup.remaining_resources == []

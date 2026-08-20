"""Provider for runtimes whose scheduler lifecycle is owned externally."""

from __future__ import annotations

import subprocess

from clio_relay.errors import ConfigurationError
from clio_relay.models import SchedulerPhase, SchedulerStatus

from .validation import _validate_scheduler_job_id


class ExternalSchedulerProvider:
    """Provider for runtimes whose scheduler lifecycle is owned externally."""

    name = "external"

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Report that no relay-owned scheduler observation is configured."""
        _validate_scheduler_job_id(scheduler_job_id)
        return SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            phase=SchedulerPhase.UNKNOWN,
            queue_position_note="scheduler observation is owned by the deployment driver",
        )

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Reject scheduler cancellation when no relay-owned provider is configured."""
        _validate_scheduler_job_id(scheduler_job_id)
        return subprocess.CompletedProcess(
            ["external-scheduler", scheduler_job_id],
            2,
            "",
            "scheduler cancellation is owned by the deployment driver",
        )

    def scheduler_cluster_name(self) -> str | None:
        """Return no scheduler-native identity for externally owned runtimes."""
        return None

    def submit_held_validation_job(self, *, job_name: str, run_seconds: int) -> str:
        """Reject live submission when scheduling is externally managed."""
        del job_name, run_seconds
        raise ConfigurationError("external scheduler providers cannot submit held validation jobs")

    def release_validation_job(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Reject release when scheduling is externally managed."""
        _validate_scheduler_job_id(scheduler_job_id)
        return subprocess.CompletedProcess(
            ["external-scheduler", "release", scheduler_job_id],
            2,
            "",
            "scheduler release is owned by the deployment driver",
        )

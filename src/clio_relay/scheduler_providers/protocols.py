"""Provider interface protocols the scheduler boundary is typed against."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable

from clio_relay.models import (
    SchedulerConnectorPlacement,
    SchedulerConnectorStepIdentity,
    SchedulerConnectorStepStatus,
    SchedulerStatus,
)


class SchedulerProvider(Protocol):
    """Provider interface for scheduler status, cancellation, and target identity."""

    name: str

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Poll scheduler status for a scheduler job id."""
        ...

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Request scheduler cancellation for a scheduler job id."""
        ...

    def scheduler_cluster_name(self) -> str | None:
        """Return the scheduler-native cluster name when one exists."""
        ...


@runtime_checkable
class SchedulerValidationProvider(SchedulerProvider, Protocol):
    """Optional provider operations used by deterministic live acceptance."""

    name: str

    def submit_held_validation_job(self, *, job_name: str, run_seconds: int) -> str:
        """Submit bounded held work and return its scheduler job id."""
        ...

    def release_validation_job(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Release a held validation job without changing any other job."""
        ...


@runtime_checkable
class SchedulerAllocationConnectorProvider(SchedulerProvider, Protocol):
    """Optional provider boundary for a connector step inside an owned allocation."""

    def connector_placement(self, scheduler_job_id: str) -> SchedulerConnectorPlacement:
        """Resolve the single exact allocation host where the connector must execute."""
        ...

    def launch_connector_step(
        self,
        scheduler_job_id: str,
        *,
        placement_host: str,
        step_marker: str,
        command: Sequence[str],
        output_path: str,
    ) -> SchedulerConnectorStepIdentity:
        """Launch an asynchronous provider-owned connector step."""
        ...

    def poll_connector_step(
        self,
        scheduler_job_id: str,
        *,
        scheduler_step_id: str,
        placement_host: str,
    ) -> SchedulerConnectorStepStatus:
        """Observe one exact connector step and its pinned placement."""
        ...

    def cancel_connector_step(
        self,
        scheduler_job_id: str,
        *,
        scheduler_step_id: str,
    ) -> subprocess.CompletedProcess[str]:
        """Cancel only one exact connector step, never its parent allocation."""
        ...

    def find_connector_step(
        self,
        scheduler_job_id: str,
        *,
        step_marker: str,
        placement_host: str,
    ) -> SchedulerConnectorStepIdentity | None:
        """Reconcile a crash-interrupted launch by its unique provider marker."""
        ...


@runtime_checkable
class SchedulerReconciliationProvider(SchedulerProvider, Protocol):
    """Optional exact-marker lookup for interrupted scheduler submissions."""

    def find_job_ids_by_marker(
        self,
        marker: str,
        *,
        submitted_after: datetime,
        scheduler_user: str,
    ) -> list[str]:
        """Return scheduler ids whose provider-native job name exactly matches marker."""
        ...

"""Scheduler names the service runtime accepts.

The runtime's own remote scripts drive the scheduler; this module only names
which schedulers those scripts support. Allocation-scoped connector steps are
retired: relay's serve runs frpc beside the service inside the job, so this
runtime refuses to start one and reports any left by an older relay as a
residual to cancel by hand.
"""

from __future__ import annotations

import re

from clio_relay.errors import ConfigurationError

_SCHEDULERS = frozenset({"external", "slurm"})
_ALLOCATION_CONNECTORS = frozenset({"slurm"})
_EXTERNAL_ALIASES = frozenset({"external", "none", "unmanaged"})


def canonical_scheduler(name: str | None) -> str:
    """Return the canonical scheduler name, or refuse one the runtime cannot drive."""
    if name is None or name.strip() == "":
        raise ConfigurationError(
            "scheduler provider must be explicit; configure external or a scheduler provider"
        )
    normalized = name.strip().lower().replace("_", "-")
    if not re.fullmatch(r"[a-z][a-z0-9-]*", normalized):
        raise ConfigurationError(f"invalid scheduler provider name: {name!r}")
    if normalized in _EXTERNAL_ALIASES:
        normalized = "external"
    if normalized not in _SCHEDULERS:
        raise ConfigurationError(f"unsupported scheduler provider: {name}")
    return normalized


def supports_allocation_connector(name: str) -> bool:
    """Return whether the scheduler can launch a connector step inside an allocation."""
    return canonical_scheduler(name) in _ALLOCATION_CONNECTORS


ALLOCATION_CONNECTOR_RETIRED = (
    "this relay no longer starts a connector inside a scheduler allocation; "
    "serve the service with the serve tool, which runs its connector beside it in the job"
)


def allocation_connector_residual(*, scheduler_job_id: str, scheduler_step_id: str) -> str:
    """Describe a connector step left by an older relay that a person must cancel."""
    return (
        "relay no longer manages connector steps inside a scheduler allocation; "
        f"step {scheduler_step_id} of job {scheduler_job_id} may still run, "
        f"cancel it with: scancel {scheduler_step_id}"
    )


def allocation_connector_intent_residual(*, scheduler_job_id: str, step_marker: str) -> str:
    """Describe a connector step an older relay may have started but never recorded."""
    return (
        "relay no longer manages connector steps inside a scheduler allocation; "
        f"a step named {step_marker} in job {scheduler_job_id} may still run, "
        f"find it with: squeue -s -j {scheduler_job_id}, and cancel it with scancel"
    )

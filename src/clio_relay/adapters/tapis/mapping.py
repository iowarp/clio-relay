"""How a relay job spec becomes a Tapis job request, and Tapis states become task states (RL46)."""

from __future__ import annotations

import math
from typing import Any, cast

from clio_relay.jobs.handles import JobState
from clio_relay.jobs.spec import JobSpec

MIB = 1024 * 1024
TAPISLOCAL = "tapislocal://exec.tapis"
# Tapis has no "no limit" for maxMinutes, so a job with no walltime asks for a year.
NO_WALLTIME_MINUTES = 365 * 24 * 60

QUEUED = frozenset(
    {
        "PENDING",
        "PROCESSING_INPUTS",
        "STAGING_INPUTS",
        "STAGING_JOB",
        "SUBMITTING_JOB",
        "QUEUED",
        "BLOCKED",
        "PAUSED",
    }
)
RUNNING = frozenset({"RUNNING", "ARCHIVING"})
STATES: dict[str, JobState] = {
    **dict.fromkeys(QUEUED, JobState.QUEUED),
    **dict.fromkeys(RUNNING, JobState.RUNNING),
    "FINISHED": JobState.COMPLETED,
    "CANCELLED": JobState.CANCELLED,
    "FAILED": JobState.FAILED,
}


def task_state(tapis_status: str) -> JobState:
    """Map a Tapis job status. An unknown one is still in progress."""
    return STATES.get(tapis_status.upper(), JobState.RUNNING)


def job_type(system: dict[str, Any]) -> str | None:
    """``BATCH`` on a Slurm system, ``FORK`` on one with no scheduler, else ``None``."""
    scheduler = str(system.get("batchScheduler") or "").upper()
    if system.get("canRunBatch") and scheduler == "SLURM":
        return "BATCH"
    if not system.get("canRunBatch") and scheduler in ("", "NONE"):
        return "FORK" if system.get("canExec", True) else None
    return None


def scheduler_options(spec: JobSpec) -> list[dict[str, str]]:
    """Slurm flags for what the Tapis request has no field for."""
    attributes = spec.attributes
    flags: list[str] = []
    if attributes.account:
        flags.append(f"-A {attributes.account}")
    if attributes.reservation_id:
        flags.append(f"--reservation={attributes.reservation_id}")
    if spec.resources.gpu_cores_per_process:
        flags.append(f"--gpus-per-task={spec.resources.gpu_cores_per_process}")
    if spec.resources.exclusive_node_use:
        flags.append("--exclusive")
    for key, value in attributes.custom_attributes.items():
        name = key.removeprefix("slurm.")
        if isinstance(value, bool):
            if value:
                flags.append(f"--{name}")
            continue
        flags.append(f"--{name}={value}")
    return [{"name": f"relay-{index}", "arg": flag} for index, flag in enumerate(flags)]


def job_request(
    spec: JobSpec,
    *,
    operation_id: str,
    app_id: str,
    app_version: str,
    system_id: str,
    system_type: str,
    job_dir: str,
    content_dir_host: str,
    launcher_arg: str,
    max_minutes: int,
) -> dict[str, Any]:
    """Build the body of ``jobs.submitJob``."""
    resources = spec.resources
    params: dict[str, Any] = {
        "appArgs": [{"name": "relay", "arg": launcher_arg}],
        "envVariables": [{"key": key, "value": value} for key, value in spec.environment.items()],
        "archiveFilter": {"includeLaunchFiles": False},
    }
    request: dict[str, Any] = {
        "name": spec.name or f"relay-{operation_id}",
        "description": description(operation_id),
        "appId": app_id,
        "appVersion": app_version,
        "jobType": system_type,
        "execSystemId": system_id,
        "execSystemExecDir": job_dir,
        "execSystemInputDir": job_dir,
        "execSystemOutputDir": job_dir,
        "archiveSystemId": system_id,
        "archiveSystemDir": job_dir,
        "archiveOnAppError": True,
        "fileInputs": [
            {
                "name": f"relay-input-{index}",
                "sourceUrl": f"{TAPISLOCAL}{content_dir_host.rstrip('/')}/{item.sha256}",
                "targetPath": item.name,
            }
            for index, item in enumerate(spec.inputs)
        ],
        "tags": ["clio-relay"],
        "parameterSet": params,
        "maxMinutes": max_minutes,
    }
    if resources.node_count is not None:
        request["nodeCount"] = resources.node_count
    cores = (resources.processes_per_node or 0) * (resources.cpu_cores_per_process or 1)
    if cores:
        request["coresPerNode"] = cores
    if resources.memory is not None:
        request["memoryMB"] = math.ceil(resources.memory / MIB)
    if system_type == "BATCH":
        if spec.attributes.queue_name:
            request["execSystemLogicalQueue"] = spec.attributes.queue_name
        params["schedulerOptions"] = scheduler_options(spec)
    return request


def max_minutes(spec: JobSpec, system: dict[str, Any], system_type: str) -> int:
    """The job's ``maxMinutes``.

    Tapis fails a job that runs past it, and when the request leaves it out Tapis
    uses 10 minutes. So a spec with no walltime asks for the most the system
    allows: the logical queue's own limit on a batch system, else a year.
    """
    duration = spec.attributes.duration
    if duration is not None:
        return max(1, math.ceil(duration.total_seconds() / 60))
    if system_type == "BATCH":
        name = spec.attributes.queue_name or system.get("batchDefaultLogicalQueue")
        for queue in cast(list[dict[str, Any]], system.get("batchLogicalQueues") or []):
            limit = queue.get("maxMinutes")
            if queue.get("name") == name and isinstance(limit, int) and limit > 0:
                return limit
    return NO_WALLTIME_MINUTES


def description(operation_id: str) -> str:
    return f"clio-relay operation {operation_id}"


def app_definition(
    *,
    app_id: str,
    app_version: str,
    system_id: str,
    container_image: str,
) -> dict[str, Any]:
    """Build the body of ``apps.createAppVersion`` for relay's app on one system."""
    return {
        "id": app_id,
        "version": app_version,
        "description": "clio-relay's launcher. Relay builds every job request for it.",
        "runtime": "ZIP",
        "containerImage": container_image,
        "strictFileInputs": False,
        "jobAttributes": {
            "execSystemId": system_id,
            "parameterSet": {"archiveFilter": {"includeLaunchFiles": False}},
        },
    }

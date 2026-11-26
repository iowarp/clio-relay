"""Check a job's layout against the partition it would run in (RL25).

The guard reads the partitions from the adapter's ``describe`` (live from the
scheduler) and refuses a job that asks for more than its partition has: more
nodes than it holds, more ranks times threads per node than its nodes' physical
cores, more memory per node than its nodes have, or more walltime than its time
limit. A queue the site does not have is refused too. A site that reports no
partitions is not checked.
"""

from __future__ import annotations

from typing import Any

from clio_relay.adapters.base import PartitionInfo, SiteInfo
from clio_relay.jobs.refusals import RefusalReason
from clio_relay.jobs.spec import JobSpec

MEMORY_SHARE = 0.9
"""The share of a node's memory a job may ask for; the rest is the system's."""


def partition_for(info: SiteInfo, queue: str | None) -> PartitionInfo | None:
    """Return the partition ``queue`` names, or the default one when it names none."""
    if queue:
        return next((found for found in info.partitions if found.name == queue), None)
    return next((found for found in info.partitions if found.default), None)


def _limits(partition: PartitionInfo) -> dict[str, Any]:
    return {
        "partition": partition.name,
        "nodes": partition.nodes,
        "idle": partition.idle,
        "physical_cores": partition.physical_cores,
        "cpus_per_node": partition.cpus_per_node,
        "memory_mb": partition.memory_mb,
        "max_time": partition.max_time,
    }


def check(spec: JobSpec, info: SiteInfo) -> tuple[RefusalReason, str, dict[str, Any]] | None:
    """Return why ``spec``'s layout does not fit, or None when it fits or cannot be checked."""
    if not info.partitions:
        return None
    queue = spec.attributes.queue_name
    partition = partition_for(info, queue)
    if partition is None:
        if not queue:
            return None
        names = [found.name for found in info.partitions]
        return (
            RefusalReason.INVALID_SPEC,
            f"the site has no partition {queue!r}; it has {', '.join(names)}",
            {"field": "attributes.queue_name", "partitions": names},
        )
    resources = spec.resources
    nodes = resources.nodes()
    ranks = resources.ranks_per_node()
    threads = resources.cpu_cores_per_process or 1
    duration = spec.attributes.duration
    problems: list[str] = []
    if partition.nodes and nodes > partition.nodes:
        problems.append(f"{nodes} nodes, but it has {partition.nodes}")
    cores = partition.physical_cores or partition.cpus_per_node
    if cores and ranks is not None and ranks * threads > cores:
        problems.append(
            f"{ranks} ranks x {threads} cores per node, but its nodes have {cores} cores"
        )
    if partition.memory_mb and resources.memory is not None:
        wanted = -(-resources.memory // 2**20)
        allowed = int(partition.memory_mb * MEMORY_SHARE)
        if wanted > allowed:
            problems.append(
                f"{wanted} MB per node, but its nodes have {partition.memory_mb} MB "
                f"(jobs get up to {allowed})"
            )
    if (
        partition.max_time_s is not None
        and duration is not None
        and duration.total_seconds() > partition.max_time_s
    ):
        problems.append(
            f"{duration.total_seconds():g} s of walltime, but its limit is {partition.max_time}"
        )
    if not problems:
        return None
    return (
        RefusalReason.INVALID_SPEC,
        f"the job does not fit partition {partition.name}: it asks for {'; '.join(problems)}. "
        "Size it to the partition, or pick another (describe(site=) lists them)",
        {"field": "resources", "limits": _limits(partition), "next_step": "resize"},
    )

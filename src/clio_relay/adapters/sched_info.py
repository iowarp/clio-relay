"""The site's scheduler facts, as JARVIS reports them (``jarvis sched info +json``).

Relay runs no scheduler reads of its own for ``describe``: JARVIS's
``jarvis.sched-info.v1`` document has the partitions and their limits, the MPI
candidates an install would offer, the resource graph's network, the site
skills and what is missing (``issues``). It is light, so it runs on the login
host. This module turns it into :class:`~clio_relay.adapters.base.PartitionInfo`
for the guard and ``describe``.
"""

from __future__ import annotations

import json
import re
from typing import Any, cast

from clio_relay.adapters.base import PartitionInfo

SCHEMA = "jarvis.sched-info.v1"
CPU_SCHEMA = "jarvis.resource-graph.cpu.v1"
"""The resource graph's ``cpu`` section: the topology JARVIS's collector read on each node."""


def report(text: str) -> dict[str, Any] | None:
    """Return the ``jarvis.sched-info.v1`` document in ``text``, or None."""
    decoder = json.JSONDecoder()
    for match in reversed(list(re.finditer(r"(?m)^\s*\{", text))):
        try:
            found, _end = decoder.raw_decode(text[match.start() :].lstrip())
        except ValueError:
            continue
        if isinstance(found, dict) and cast(dict[str, Any], found).get("schema_version") == SCHEMA:
            return cast(dict[str, Any], found)
    return None


def _int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _names(value: object) -> list[str]:
    if isinstance(value, str):
        return [item for item in value.split(",") if item and item.lower() not in ("(null)", "n/a")]
    if isinstance(value, list):
        return [str(item) for item in cast(list[object], value) if item]
    return []


def partitions(document: dict[str, Any] | None) -> list[PartitionInfo]:
    """Return the partitions JARVIS reported, one entry each."""
    listed: object = (document or {}).get("partitions")
    if not isinstance(listed, list):
        return []
    found: list[PartitionInfo] = []
    for raw in cast(list[object], listed):
        if not isinstance(raw, dict):
            continue
        entry = cast(dict[str, Any], raw)
        name = entry.get("partition") or entry.get("name")
        if not isinstance(name, str) or not name:
            continue
        max_time = entry.get("max_time")
        found.append(
            PartitionInfo(
                name=name,
                default=bool(entry.get("default")),
                nodes=_int(entry.get("total_nodes")) or 0,
                cpus_per_node=_int(entry.get("cpus_per_node")),
                sockets=_int(entry.get("sockets_per_node")),
                cores_per_socket=_int(entry.get("cores_per_socket")),
                threads_per_core=_int(entry.get("threads_per_core")),
                physical_cores=_int(entry.get("physical_cores_per_node")),
                heterogeneous=entry.get("heterogeneous") is True,
                memory_mb=_int(entry.get("memory_mb_per_node")),
                max_time=str(max_time) if max_time is not None else None,
                max_time_s=_int(entry.get("max_time_s")),
                allocated=_int(entry.get("allocated_nodes")) or 0,
                idle=_int(entry.get("idle_nodes")) or 0,
                other=_int(entry.get("other_nodes")) or 0,
                gres=_names(entry.get("gres")),
            )
        )
    return found


def site_facts(document: dict[str, Any] | None) -> dict[str, Any]:
    """Return what ``describe`` shows of the document besides the partitions."""
    if not document:
        return {}
    keys = ("provider", "providers_supported", "account", "in_allocation", "observed_at")
    facts: dict[str, Any] = {key: document.get(key) for key in keys if key in document}
    facts["site_skills"] = document.get("site_skills") or []
    facts["issues"] = document.get("issues") or []
    return facts


def graph_cpu(document: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return the resource graph's ``cpu`` section the document carries, unaltered, or None.

    None before a collection, and from a JARVIS older than the section.
    """
    found: object = (document or {}).get("cpu")
    if isinstance(found, dict) and cast(dict[str, Any], found).get("schema_version") == CPU_SCHEMA:
        return cast(dict[str, Any], found)
    return None


def compute_cores(
    found: list[PartitionInfo], cpu: dict[str, Any] | None
) -> dict[str, dict[str, Any]]:
    """Return each partition's compute-node cores, as JARVIS knows them.

    A partition's own topology is ``sinfo``'s; without it, the smallest node
    the resource graph's ``cpu`` section recorded stands in, as JARVIS's plan
    does. ``source`` says which, and is None when JARVIS knows neither. The
    host relay reaches (a login node, often) is never asked: its cores are
    not a compute node's.
    """
    smallest = cast(dict[str, Any], (cpu or {}).get("smallest") or {})
    graph = {
        "physical_cores": _int(smallest.get("physical_cores")),
        "threads_per_core": _int(smallest.get("threads_per_core")),
        "cpus": _int(smallest.get("cpus")),
        "heterogeneous": (cpu or {}).get("heterogeneous") is True,
        "source": "resource_graph.cpu",
    }
    cores: dict[str, dict[str, Any]] = {}
    for partition in found:
        if partition.physical_cores:
            cores[partition.name] = {
                "physical_cores": partition.physical_cores,
                "threads_per_core": partition.threads_per_core,
                "cpus": partition.cpus_per_node,
                "heterogeneous": partition.heterogeneous,
                "source": "sinfo",
            }
        elif graph["physical_cores"]:
            cores[partition.name] = dict(graph)
        else:
            cores[partition.name] = {
                "physical_cores": None,
                "threads_per_core": None,
                "cpus": partition.cpus_per_node,
                "heterogeneous": partition.heterogeneous,
                "source": None,
            }
    return cores


def threads_per_core(
    cores: dict[str, dict[str, Any]], found: list[PartitionInfo], queue: str | None
) -> int | None:
    """Return the hardware threads per core of ``queue``'s nodes (the default partition's
    when it names none), or None when JARVIS does not know them."""
    name = queue or next((partition.name for partition in found if partition.default), None)
    return _int((cores.get(name) or {}).get("threads_per_core")) if name else None

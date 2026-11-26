"""The site's scheduler facts, as JARVIS reports them (``jarvis sched info +json``).

JARVIS's ``jarvis.sched-info.v1`` document has the partitions and their
limits, the MPI candidates an install would offer, the resource graph's
network, the site skills and what is missing (``issues``). It is light, so it
runs on the login host. This module turns it into
:class:`~clio_relay.adapters.base.PartitionInfo` for the guard and ``describe``.

Until relay places JARVIS on a Slurm site, ``describe`` runs the one ``sinfo``
query JARVIS's report rests on (:data:`SINFO_FORMAT`) and reads it as JARVIS
does (:func:`sinfo_partitions`), so the first describe has the real partitions.
JARVIS's report replaces it once JARVIS is there.
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
        raw_limits: object = entry.get("limits")
        limits = cast(dict[str, Any], raw_limits) if isinstance(raw_limits, dict) else {}
        min_nodes = _int(limits.get("min_nodes"))
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
                # Slurm's MinNodes=0 means no minimum: a job still has one node.
                min_nodes=max(min_nodes, 1) if min_nodes is not None else None,
                # "UNLIMITED" is no per-job limit.
                max_nodes=_int(limits.get("max_nodes")),
                allocated=_int(entry.get("allocated_nodes")) or 0,
                idle=_int(entry.get("idle_nodes")) or 0,
                other=_int(entry.get("other_nodes")) or 0,
                gres=_names(entry.get("gres")),
            )
        )
    return found


SINFO_FORMAT = "%P|%a|%F|%l|%c|%m|%G|%X|%Y|%Z|%s"
"""The ``sinfo --noheader --format`` JARVIS's ``sched info`` reads the partitions with.

The last field, ``%s``, is the nodes one job may ask for (``min-max``), which
JARVIS reads from ``scontrol show partition`` into each partition's ``limits``.
"""


def _count(text: str) -> int | None:
    text = text.rstrip("+")
    return int(text) if text.isdigit() else None


def _job_size(text: str) -> dict[str, Any]:
    """``limits`` of sinfo's job size (``1-infinite``, ``2-64``, ``4``), in JARVIS's shape."""
    low, _, high = text.strip().partition("-")
    if not low.isdigit():
        return {}
    high = high or low
    if high.lower() in ("infinite", "unlimited"):
        return {"min_nodes": int(low), "max_nodes": "UNLIMITED"}
    return {"min_nodes": int(low), "max_nodes": int(high)} if high.isdigit() else {}


def _seconds(text: str) -> int | None:
    """Seconds of a Slurm time limit (``[days-]hours:minutes:seconds``), None for none."""
    text = text.strip()
    if not text or text.lower() in ("infinite", "unlimited", "n/a"):
        return None
    days, _, clock = text.rpartition("-")
    parts = clock.split(":")
    if not (days == "" or days.isdigit()) or not all(part.isdigit() for part in parts):
        return None
    numbers = [int(part) for part in parts]
    if len(numbers) > 3:
        return None
    if days:
        # days-hours[:minutes[:seconds]]
        numbers += [0] * (3 - len(numbers))
    elif len(numbers) < 3:
        # minutes[:seconds]
        numbers = [0, *numbers, *([0] * (2 - len(numbers)))]
    hours, minutes, seconds = numbers
    return ((int(days or 0) * 24 + hours) * 60 + minutes) * 60 + seconds


def sinfo_partitions(text: str) -> list[dict[str, Any]]:
    """Return the partitions of ``sinfo``'s :data:`SINFO_FORMAT` lines, as JARVIS lists them.

    One entry per partition, in ``jarvis.sched-info.v1``'s shape. A partition
    with several node configurations has several lines; its per-node figures
    are the smallest node's, and it is ``heterogeneous``. So is one whose
    figure ends in sinfo's ``+``, which marks the smallest of differing nodes.
    """
    merged: dict[str, dict[str, Any]] = {}
    for raw in text.splitlines():
        fields = [field.strip() for field in raw.split("|")]
        if len(fields) < 4 or not fields[0]:
            continue
        counts = fields[2].split("/")
        if len(counts) != 4 or not all(item.isdigit() for item in counts):
            continue
        name = fields[0].rstrip("*")
        seen = name in merged
        entry = merged.setdefault(
            name,
            {
                "partition": name,
                "default": fields[0].endswith("*"),
                "available": fields[1],
                "allocated_nodes": 0,
                "idle_nodes": 0,
                "other_nodes": 0,
                "total_nodes": 0,
                "max_time": fields[3],
                "max_time_s": _seconds(fields[3]),
                "cpus_per_node": None,
                "sockets_per_node": None,
                "cores_per_socket": None,
                "threads_per_core": None,
                "physical_cores_per_node": None,
                "memory_mb_per_node": None,
                "gres": None,
                "heterogeneous": False,
            },
        )
        keys = ("allocated_nodes", "idle_nodes", "other_nodes", "total_nodes")
        for key, value in zip(keys, counts, strict=True):
            entry[key] += int(value)
        if len(fields) >= 11 and "limits" not in entry:
            entry["limits"] = _job_size(fields[10])
        if len(fields) < 7:
            continue
        line: dict[str, int | None] = {
            "cpus_per_node": _count(fields[4]),
            "memory_mb_per_node": _count(fields[5]),
        }
        if len(fields) >= 10:
            line["sockets_per_node"] = _count(fields[7])
            line["cores_per_socket"] = _count(fields[8])
            line["threads_per_core"] = _count(fields[9])
            sockets, cores = line["sockets_per_node"], line["cores_per_socket"]
            line["physical_cores_per_node"] = sockets * cores if sockets and cores else None
        if any(fields[index].endswith("+") for index in range(4, min(len(fields), 10))):
            entry["heterogeneous"] = True
        for key, value in line.items():
            current = entry[key]
            if not seen or current is None:
                entry[key] = value
            elif value is not None and value != current:
                entry["heterogeneous"] = True
                entry[key] = min(current, value)
        if entry["gres"] is None and fields[6] not in ("", "(null)"):
            entry["gres"] = fields[6]
    return list(merged.values())


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

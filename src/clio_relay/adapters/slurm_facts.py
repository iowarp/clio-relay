"""What a Slurm site's partitions and MPI offer, read live through the site connection.

An adapter that reaches a Slurm login host runs :data:`SCRIPT` there as part of
``describe`` and passes the output to :func:`partitions` and :func:`mpi`. Every
command is a short read (``sinfo``, ``scontrol show config``, ``srun
--mpi=list``, ``mpiexec --version``, ``ompi_info``), bounded by ``timeout``
when the host has it, so the login node does no real work.

The output is ``@@name`` sections, as :func:`clio_relay.adapters.ssh.scripts.parse_sections`
reads them.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, cast

from clio_relay import mpi_network
from clio_relay.adapters.base import PartitionInfo

SINFO_FORMAT = "%P|%D|%c|%X|%Y|%Z|%m|%l|%F|%G|%f"
"""Partition, nodes, CPUs, sockets, cores per socket, threads per core, memory (MB),
time limit, allocated/idle/other/total, generic resources, features."""

_CONFIG_KEYS = (
    "SelectType",
    "SelectTypeParameters",
    "TaskPlugin",
    "MpiDefault",
    "MpiParams",
)

SCRIPT = (
    'T=""; command -v timeout >/dev/null 2>&1 && T="timeout 20"\n'
    'echo "@@partitions"\n'
    f"$T sinfo -h -o '{SINFO_FORMAT}' 2>/dev/null\n"
    'echo "@@slurm_config"\n'
    "$T scontrol show config 2>/dev/null | grep -E "
    f"'^({'|'.join(_CONFIG_KEYS)}) '\n"
    'echo "@@srun_mpi"\n'
    "$T srun --mpi=list 2>&1 | head -20\n"
    "if command -v mpiexec >/dev/null 2>&1; then\n"
    '  echo "@@mpiexec $(command -v mpiexec)"\n'
    "  $T mpiexec --version 2>&1 | head -3\n"
    "fi\n"
    "if command -v ompi_info >/dev/null 2>&1; then\n"
    '  echo "@@ompi"\n'
    "  $T ompi_info --parsable 2>/dev/null | awk -F: "
    '\'$1 == "mca" && ($2 == "plm" || $2 == "ras" || $2 == "pmix") '
    '{ print $2 ":" $3 }\' | sort -u\n'
    "fi\n"
    + mpi_network.SHELL
    + 'py=$(command -v python3 2>/dev/null) && { echo "@@mpi_network"; relay_mpi_network "$py"; }\n'
)
"""The shell text that reports the partitions and MPI facts, for a Slurm site.

``@@mpi_network`` is the network the site script names for MPI's TCP channels
(:mod:`clio_relay.mpi_network`), found live."""


def seconds(limit: str) -> int | None:
    """Return a Slurm time limit in seconds, or None when it is unlimited or unknown.

    Slurm writes ``D-HH:MM:SS``, ``HH:MM:SS``, ``MM:SS`` or ``D-HH``, and
    ``infinite``, ``UNLIMITED`` or ``n/a``.
    """
    text = limit.strip()
    if not text or text.lower() in {"infinite", "unlimited", "n/a", "none"}:
        return None
    days, _, rest = text.rpartition("-") if "-" in text else ("", "", text)
    parts = rest.split(":")
    if not all(part.isdigit() for part in parts) or (days and not days.isdigit()):
        return None
    numbers = [int(part) for part in parts]
    if days:
        # D-HH, D-HH:MM, D-HH:MM:SS
        numbers += [0] * (3 - len(numbers))
        hours, minutes, secs = numbers[:3]
        return ((int(days) * 24 + hours) * 60 + minutes) * 60 + secs
    if len(numbers) == 3:
        hours, minutes, secs = numbers
    elif len(numbers) == 2:
        hours, minutes, secs = 0, numbers[0], numbers[1]
    else:
        hours, minutes, secs = 0, numbers[0], 0
    return (hours * 60 + minutes) * 60 + secs


def _number(text: str) -> int | None:
    match = re.match(r"\s*(\d+)", text)
    return int(match.group(1)) if match else None


def _listing(text: str) -> list[str]:
    value = text.strip()
    if not value or value.lower() in {"(null)", "n/a"}:
        return []
    return [item for item in value.split(",") if item]


def _least(first: int | None, second: int | None) -> int | None:
    if first is None:
        return second
    if second is None:
        return first
    return min(first, second)


def partitions(lines: Sequence[str]) -> list[PartitionInfo]:
    """Return the partitions ``sinfo -o SINFO_FORMAT`` printed, one entry each.

    ``sinfo`` prints a row per group of alike nodes, so a partition with mixed
    nodes has several rows. Its node counts are summed, and its per-node
    figures are the smallest row's: what every node of the partition has.
    """
    found: dict[str, PartitionInfo] = {}
    for line in lines:
        fields = line.strip().split("|")
        if len(fields) < 9 or not fields[0]:
            continue
        name = fields[0].rstrip("*")
        counts = [_number(part) or 0 for part in fields[8].split("/")]
        allocated, idle, other, total = (counts + [0, 0, 0, 0])[:4]
        sockets = _number(fields[3])
        cores = _number(fields[4])
        row = PartitionInfo(
            name=name,
            default=fields[0].endswith("*"),
            nodes=total or (_number(fields[1]) or 0),
            cpus_per_node=_number(fields[2]),
            sockets=sockets,
            cores_per_socket=cores,
            threads_per_core=_number(fields[5]),
            physical_cores=sockets * cores if sockets and cores else None,
            memory_mb=_number(fields[6]),
            max_time=fields[7].strip() or None,
            max_time_s=seconds(fields[7]),
            allocated=allocated,
            idle=idle,
            other=other,
            gres=_listing(fields[9]) if len(fields) > 9 else [],
            features=_listing(fields[10]) if len(fields) > 10 else [],
        )
        seen = found.get(name)
        if seen is None:
            found[name] = row
            continue
        found[name] = seen.model_copy(
            update={
                "default": seen.default or row.default,
                "nodes": seen.nodes + row.nodes,
                "cpus_per_node": _least(seen.cpus_per_node, row.cpus_per_node),
                "physical_cores": _least(seen.physical_cores, row.physical_cores),
                "memory_mb": _least(seen.memory_mb, row.memory_mb),
                "allocated": seen.allocated + row.allocated,
                "idle": seen.idle + row.idle,
                "other": seen.other + row.other,
                "gres": sorted({*seen.gres, *row.gres}),
                "features": sorted(set(seen.features) & set(row.features)),
            }
        )
    return list(found.values())


def mpi(sections: Mapping[str, Sequence[str]]) -> dict[str, Any]:
    """Return the site's MPI facts: Slurm's MPI plugins and the MPI on the login PATH.

    JARVIS's launcher takes the MPI its Spack install chose (``JARVIS_MPI_*``);
    these facts tell the person, and the agent sizing a run, what the site
    offers: ``srun --mpi`` plugin types, Slurm's default, and the ``mpiexec``
    on the PATH with its flavour and prefix.
    """
    config: dict[str, str] = {}
    for line in sections.get("slurm_config", []):
        key, _, value = line.partition("=")
        if key.strip() and value.strip():
            config[key.strip()] = value.strip()
    plugins = _plugins(sections.get("srun_mpi", []))
    facts: dict[str, Any] = {
        "slurm_mpi_default": config.get("MpiDefault"),
        "slurm_mpi_plugins": plugins,
        "slurm_mpi_params": config.get("MpiParams"),
        "select_type": config.get("SelectType"),
        "select_type_parameters": config.get("SelectTypeParameters"),
        "task_plugin": config.get("TaskPlugin"),
    }
    mpiexec = sections.get("mpiexec")
    if mpiexec:
        path = mpiexec[0].strip()
        banner = " ".join(line.strip() for line in mpiexec[1:] if line.strip())
        facts["mpiexec"] = path
        facts["mpiexec_version"] = banner or None
        facts["mpi_type"] = _flavour(banner)
        bin_dir, _, _ = path.rpartition("/")
        prefix = bin_dir.rpartition("/")[0] if bin_dir.endswith("/bin") else ""
        facts["mpi_prefix"] = prefix or None
    ompi = [line.strip() for line in sections.get("ompi", []) if line.strip()]
    if ompi:
        facts["ompi_components"] = ompi
    network = mpi_network.facts(list(sections.get("mpi_network", [])))
    if network:
        facts["network"] = network
    return facts


QUEUE_FORMAT = "%P|%r|%S|%D|%l"
"""A pending job's partition, reason, estimated start, nodes and time limit."""

IDLE_FORMAT = "%P|%a|%F|%l"
"""Partition, availability, allocated/idle/other/total nodes, time limit."""


def queue_script(native_id: str) -> str:
    """Return the shell text that reports a pending Slurm job's place in the queue.

    It prints ``@@queue`` (``squeue`` for the job, only while it is pending)
    and then ``@@idle`` (``sinfo`` per partition), and ``@@alive``: squeue's
    exit code and the job's state in any state (:func:`scheduler_state`). All
    are short reads, and nothing prints for a job that is not Slurm's.
    """
    if not native_id.isdigit():
        return ""
    return (
        "if command -v squeue >/dev/null 2>&1; then\n"
        '  T=""; command -v timeout >/dev/null 2>&1 && T="timeout 15"\n'
        f"  qline=$($T squeue -h -j {native_id} -t PD -o '{QUEUE_FORMAT}' 2>/dev/null)\n"
        '  if [ -n "$qline" ]; then\n'
        '    echo "@@queue"; printf "%s\\n" "$qline"\n'
        f"    echo \"@@idle\"; $T sinfo -h -o '{IDLE_FORMAT}' 2>/dev/null\n"
        '    qstart=$(printf "%s" "$qline" | cut -d"|" -f3)\n'
        '    echo "@@clock"; date +%s; date -d "$qstart" +%s 2>/dev/null\n'
        "  fi\n"
        f"  alive=$($T squeue -h -j {native_id} -o %T 2>&1); arc=$?\n"
        '  echo "@@alive"; echo "$arc"; printf "%s\\n" "$alive"\n'
        "fi\n"
    )


def scheduler_state(sections: Mapping[str, Sequence[str]]) -> str | None:
    """Return Slurm's state for the job from :func:`queue_script`'s ``@@alive``.

    ``"GONE"`` when Slurm no longer knows the job id, the state name (such as
    ``PENDING`` or ``CANCELLED``) while it does, and None when squeue did not
    answer.
    """
    lines = [line.strip() for line in sections.get("alive", []) if line.strip()]
    if not lines:
        return None
    code, rest = lines[0], lines[1:]
    if any("invalid job id" in line.lower() for line in rest):
        return "GONE"
    if code != "0":
        return None
    return rest[0].split()[0].upper() if rest else "GONE"


def queue(sections: Mapping[str, Sequence[str]]) -> dict[str, Any] | None:
    """Return where a pending job stands, from :func:`queue_script`'s sections.

    The result has the job's ``partition``, Slurm's ``reason`` and
    ``estimated_start`` (None while Slurm has no estimate), ``starts_in_s``
    (seconds from now to that estimate, by the site's clock), and
    ``idle_partitions``: the other partitions that are up with idle nodes, each
    with ``idle`` nodes, its ``max_time`` and whether the job ``fits`` (enough
    idle nodes and a long enough time limit). None when the job is not pending.
    """
    rows = [line.strip() for line in sections.get("queue", []) if line.strip()]
    if not rows:
        return None
    fields = (rows[0].split("|") + [""] * 5)[:5]
    partition, reason, start, nodes_text, limit = (field.strip() for field in fields)
    nodes = _number(nodes_text) or 1
    needed = seconds(limit)
    idle: dict[str, dict[str, Any]] = {}
    for line in sections.get("idle", []):
        parts = line.strip().split("|")
        if len(parts) < 4 or not parts[0]:
            continue
        name = parts[0].rstrip("*")
        if name == partition or parts[1].strip().lower() != "up":
            continue
        counts = [_number(part) or 0 for part in parts[2].split("/")]
        free = (counts + [0, 0])[1]
        entry = idle.setdefault(name, {"partition": name, "idle": 0, "max_time": parts[3].strip()})
        entry["idle"] += free
    found: list[dict[str, Any]] = []
    for entry in idle.values():
        if entry["idle"] <= 0:
            continue
        most = seconds(str(entry["max_time"]))
        long_enough = needed is None or most is None or most >= needed
        found.append({**entry, "fits": entry["idle"] >= nodes and long_enough})
    found.sort(key=lambda item: (not item["fits"], -item["idle"]))
    estimated = start if start and start.upper() not in {"N/A", "NONE"} else None
    clock = [_number(line) for line in sections.get("clock", []) if line.strip()]
    starts_in: int | None = None
    if estimated and len(clock) >= 2 and clock[0] is not None and clock[1] is not None:
        starts_in = max(0, clock[1] - clock[0])
    return {
        "partition": partition or None,
        "reason": reason or None,
        "estimated_start": estimated,
        "starts_in_s": starts_in,
        "nodes": nodes,
        "time_limit": limit or None,
        "idle_partitions": found,
    }


def queue_message(facts: Mapping[str, Any]) -> str:
    """Say in one line where a pending job stands, for an operation's message."""
    partition = facts.get("partition") or "its partition"
    text = f"waiting in the {partition} queue"
    if facts.get("reason"):
        text += f" ({facts['reason']})"
    start = facts.get("estimated_start")
    text += (
        f"; Slurm estimates the start at {start}" if start else "; Slurm has no start estimate yet"
    )
    idle = cast(list[dict[str, Any]], facts.get("idle_partitions") or [])
    fitting = [item for item in idle if item.get("fits")]
    if fitting:
        names = ", ".join(f"{item['partition']} ({item['idle']} idle nodes)" for item in fitting)
        text += f"; partitions with room for it now: {names}"
    return text


def _plugins(lines: Sequence[str]) -> list[str]:
    """Return the plugin types ``srun --mpi=list`` printed.

    Slurm prints a heading, then one type per line (``srun: pmi2`` before 21.08),
    then ``specific pmix plugin versions available: pmix_v4,...``.
    """
    found: set[str] = set()
    for line in lines:
        text = line.strip().removeprefix("srun:").strip()
        if "available:" in text:
            versions = text.partition("available:")[2]
            found.update(item.strip() for item in versions.split(",") if item.strip())
        elif re.fullmatch(r"[a-z][a-z0-9_.-]*", text):
            found.add(text)
    return sorted(found)


def _flavour(banner: str) -> str | None:
    text = banner.lower()
    for marker, name in (
        ("open mpi", "openmpi"),
        ("open-mpi", "openmpi"),
        ("intel(r) mpi", "intel_mpi"),
        ("mvapich", "mvapich"),
        ("hydra", "mpich"),
        ("mpich", "mpich"),
    ):
        if marker in text:
            return name
    return None

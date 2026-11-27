"""Read :func:`scripts.describe_script`'s output into the site's :class:`SiteInfo`.

The ``ssh`` adapter runs the script on the site over ``ssh``. The ``local``
adapter runs the same script on its own POSIX host, which is how a worker site
(relay's worker on the cluster) reports the same scheduler, partition, MPI and
resource graph facts as an ``ssh`` site.
"""

from __future__ import annotations

import posixpath
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

from clio_relay import jarvis_submit, site_outputs
from clio_relay.adapters import sched_info
from clio_relay.adapters.base import PartitionInfo, SiteInfo, Verb
from clio_relay.adapters.ssh import scripts
from clio_relay.jobs.handles import utc_now


@dataclass(frozen=True)
class Described:
    """What the describe script said, as relay reports it."""

    info: SiteInfo
    compute_cores: dict[str, dict[str, Any]]
    partitions: list[PartitionInfo]


def site_info(
    text: str,
    *,
    adapter: str,
    address: str | None,
    verbs: Iterable[Verb],
    remote_dir: str,
    jarvis: str,
    graph_setting: str,
    outputs_dir: str | None,
) -> Described:
    """Return the site's facts from the describe script's ``text``."""
    sections = scripts.parse_sections(text)

    def first(key: str) -> str | None:
        values = [value for value in sections.get(key, []) if value.strip()]
        return values[0].strip() if values else None

    cpus = first("cpus")
    queues = sorted({line.strip() for line in sections.get("queues", []) if line.strip()})
    sched_text = "\n".join(sections.get("sched", []))
    sched = sched_info.report(sched_text)
    sinfo = None
    if sched is None and "sinfo" in sections:
        listed = sched_info.sinfo_partitions("\n".join(sections["sinfo"]))
        sinfo = {"partitions": listed} if listed else None
    partitions = sched_info.partitions(sched or sinfo)
    cpu = sched_info.graph_cpu(sched)
    compute_cores = sched_info.compute_cores(partitions, cpu)
    scheduler = first("scheduler") or "none"
    graph_text = "\n".join(sections.get("graph", []))
    pipeline = jarvis_submit.graph_pipeline(scheduler)
    job: dict[str, Any] | None = pipeline.get("scheduler")
    where = f"on {job['nodes']} compute nodes for up to {job['time']}" if job else "here"
    summary = (
        f"Relay's network collection job runs {where} with JARVIS and collects the "
        "networks it sees into the site's resource graph."
    )
    source = first("graph_source") or ("setting" if graph_setting else "relay")
    graph: dict[str, Any] = {
        "path": first("graph_path"),
        # setting: the site's jarvis_graph; site: the graph the site publishes as
        # $JARVIS_RESOURCE_GRAPH; relay: the home's own, which only collection fills.
        "source": source,
        "published": source != "relay",
        # JARVIS only reads a named graph; what it records goes to its own graph
        # here, under the CLIO site home, over the named one.
        "site_home": first("site_home"),
        "decisions": first("decisions_path"),
        "net": jarvis_submit.graph_report(graph_text),
    }
    if source == "site":
        graph["note"] = (
            "The site publishes this resource graph ($JARVIS_RESOURCE_GRAPH) and every "
            "relay JARVIS call uses it. JARVIS only reads it, keeping what it records "
            "under site_home; nothing to collect"
        )
        if "graph" not in sections:
            graph["note"] += (
                ". Its net section is read with relay's JARVIS, so net shows once "
                "relay has placed JARVIS on the site; null until then is not empty"
            )
    else:
        unset = (
            "The site's jarvis_graph setting is relay: relay's own graph, whatever the "
            "site publishes. "
            if graph_setting == jarvis_submit.OWN_GRAPH
            else "The site's jarvis_graph setting names this graph. "
            if source == "setting"
            else "The site publishes no resource graph ($JARVIS_RESOURCE_GRAPH is unset). "
        )
        graph["collect"] = {
            "exec": scripts.graph_collect_command(remote_dir, scheduler, jarvis, graph_setting),
            "pipeline": pipeline,
            "plan": {"kind": "resource_graph", "summary": summary, "submits": job is not None},
            "note": (
                unset + "Relay's network collection job collects the networks on compute "
                "nodes; run this with exec (and this plan on a guarded site) once the "
                "person approves, then describe again"
            ),
        }
    if graph["net"] is None and graph_text.strip():
        graph["detail"] = graph_text.strip()[-500:]
    info = SiteInfo(
        adapter=adapter,
        address=address,
        scheduler=scheduler if scheduler != "none" else None,
        queues=queues or [partition.name for partition in partitions],
        partitions=partitions,
        mpi=cast(dict[str, Any], (sched or {}).get("mpi") or {}),
        facts={
            "host": first("host"),
            "os": first("os"),
            "machine": first("machine"),
            # The host relay reaches, a login node on most sites: not a compute fact.
            "login_cpus": int(cpus) if cpus and cpus.isdigit() else None,
            "compute_cores": compute_cores,
            "cpu": cpu,
            "home": first("home"),
            **site_outputs.dir_facts(remote_dir, outputs_dir),
            "resource_graph": graph,
            "sched": _sched_facts(sched, sections, sched_text, sinfo),
            "jarvis": _jarvis_facts(remote_dir, jarvis, graph_setting),
        },
        verbs=sorted(verbs),
    )
    return Described(info=info, compute_cores=compute_cores, partitions=partitions)


def _sched_facts(
    sched: dict[str, Any] | None,
    sections: dict[str, list[str]],
    text: str,
    sinfo: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if sched is not None:
        return sched_info.site_facts(sched)
    if "nosched" in sections:
        if sinfo is not None:
            return {
                "source": "sinfo",
                "observed_at": utc_now().isoformat(),
                "note": (
                    "relay has not placed JARVIS on this site yet, so the partitions are "
                    "the sinfo query JARVIS's sched info reads, as of observed_at; "
                    "JARVIS's report replaces them once JARVIS is placed"
                ),
            }
        return {"note": "relay has not placed JARVIS on this site yet; its first run does"}
    return {"note": "jarvis sched info gave no report", "detail": text.strip()[-500:] or None}


def _jarvis_facts(remote_dir: str, jarvis: str, graph_setting: str) -> dict[str, Any]:
    home = remote_dir.rstrip("/") or "/"
    return {
        "exec": scripts.jarvis_command(remote_dir, jarvis, graph_setting),
        "read": ["jarvis"],
        "spack_root": posixpath.join(home, jarvis_submit.SPACK_DIR),
        "note": (
            "exec_read + sched info|rg facts show|rg skill list|show NAME +json: no plan. "
            "exec + rg facts answer|rg skill set|rg net set|spack prepare --root <spack_root> "
            "--site_spack <root> +json, once the person agrees; a guarded site asks a plan"
        ),
    }

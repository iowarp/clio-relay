"""The question relay asks before JARVIS installs a package's software on a site.

It is built from JARVIS's install report (``ppl install +check +json``): what is
reused, what is built, which choices JARVIS replays from the site's decisions
record, and how many questions follow.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from clio_relay.jarvis_report import (
    applied_decisions,
    build_facts,
    choice_issues,
    install_summary,
)


def install_message(
    report: Mapping[str, Any],
    *,
    site: str,
    package: str,
    names: Mapping[str, str] | None = None,
    walltime: str | None = None,
    run: bool = True,
) -> str:
    """Return the approval question for installing the package's software on the site.

    ``walltime`` is the install job's, when the adapter reported it; ``run`` is
    False for an install ahead of any run, which launches nothing after it. The choices
    JARVIS replays from its decisions record (:func:`applied_decisions`) are
    named, so the person sees what is reused without being asked again.
    """
    summary = install_summary(report, names)
    applied = applied_decisions(report)
    asked = len(choice_issues(report))
    found = build_facts(report, names)
    reuse = ""
    if applied:
        named = ", ".join(
            f"{item.get('subject') or item.get('package') or 'a choice'} {item['constraint']}"
            for item in applied
        )
        where = report.get("decisions_path")
        kept = f" in {where}" if isinstance(where, str) and where else ""
        reuse = f" JARVIS applies the {named} chosen for {site} before (kept{kept})."
    before = f"before {package} runs" if run else f"for {package}"
    then = "then the run goes ahead" if run else "and nothing runs after it"
    last = "before the run" if run else "ahead of any run; nothing runs after it"
    if not found and asked:
        return (
            f"JARVIS needs {asked} choice(s) from you {before} on {site}; nothing has to be "
            f"installed. It applies your answers in a short job on the site, {then}.{reuse} "
            f"Answer JARVIS's questions for {package} on {site}?"
        )
    if not found:
        return (
            f"JARVIS applies its recorded choices for {site} in a short job {before}; "
            f"nothing has to be installed.{reuse} Apply them for {package} on {site}?"
        )
    parts = [_finding(item, site=site) for item in found]
    if summary["reuse"]:
        parts.append(f"Already on {site}: " + ", ".join(summary["reuse"]) + ".")
    if asked:
        parts.append(f"JARVIS has {asked} question(s) for you after this.")
    if reuse:
        parts.append(reuse.strip())
    job = f"an install job of up to {walltime}" if walltime else "an install job"
    parts.append(_where_built(report, site=site, job=job, last=last))
    parts.append(f"Install the software {package} needs on {site}?")
    return " ".join(parts)


def _finding(item: Mapping[str, Any], *, site: str) -> str:
    """Say what JARVIS's check found for one package and what it does about it.

    Only what JARVIS reported is said: the spec it looked for, how many of the
    spec's Spack packages the site already has, the MPI it builds against and
    its build estimate.
    """
    what = item["spec"] or item["package"]
    if item["method"] not in (None, "", "spack"):
        return f"JARVIS finds no {what} on {site} and installs it with {item['method']}."
    sentence = f"JARVIS's Spack check finds no installed {what} on {site}, so it builds one"
    total, present = item.get("total"), item.get("present")
    if isinstance(total, int) and isinstance(present, int):
        sentence += (
            f": {present} of the {total} Spack packages it needs are on {site} already "
            f"and {total - present} are built"
        )
    else:
        sentence += " with Spack"
    if item.get("estimate"):
        sentence += f", {item['estimate']} by JARVIS's estimate"
    sentence += "."
    mpi = _mpi_text(item.get("mpi"))
    return f"{sentence} It builds against {mpi}." if mpi else sentence


def _mpi_text(mpi: object) -> str | None:
    """Name the MPI JARVIS builds against: the site module when it comes from one."""
    if not isinstance(mpi, dict):
        return None
    facts = cast(dict[str, Any], mpi)
    name = facts.get("name")
    if not isinstance(name, str) or not name:
        return None
    version = facts.get("version")
    label = f"{name} {version}" if isinstance(version, str) and version else name
    modules = facts.get("modules")
    if isinstance(modules, list) and modules:
        listed = ", ".join(str(module) for module in cast(list[object], modules))
        return f"{label}, the site module {listed}"
    prefix = facts.get("prefix")
    return f"{label} at {prefix}" if isinstance(prefix, str) and prefix else label


def install_meanings(
    report: Mapping[str, Any],
    *,
    site: str,
    package: str,
    names: Mapping[str, str] | None = None,
    run: bool = True,
) -> tuple[str, str, str]:
    """Return the install approval's title and what Approve and Decline each do.

    Built from the same JARVIS report as :func:`install_message`. Relay
    recommends neither answer; the person decides.
    """
    found = build_facts(report, names)
    then = f", then {package} runs." if run else "; nothing runs after it."
    stops = f" and {package} does not run." if run else "."
    if not found:
        return (
            f"JARVIS's choices for {package} on {site}",
            f"JARVIS applies the choices in a short job on {site}{then}",
            f"Nothing changes on {site}{stops}",
        )
    actions: list[str] = []
    for item in found:
        what = item["spec"] or item["package"]
        if item["method"] in (None, "", "spack"):
            took = f" ({item['estimate']})" if item.get("estimate") else ""
            actions.append(f"builds {what} with Spack{took}")
        else:
            actions.append(f"installs {what} with {item['method']}")
    return (
        f"Software for {package} on {site}",
        f"JARVIS {_joined(actions)} on {site}{_where_short(report)}{then}",
        f"Nothing is built or installed on {site}{stops}",
    )


def _joined(items: list[str]) -> str:
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def _where_short(report: Mapping[str, Any]) -> str:
    """Where JARVIS builds, as a phrase after the site, when its placement says."""
    placement: object = report.get("placement")
    build = cast(dict[str, Any], placement).get("build") if isinstance(placement, dict) else None
    choice = cast(dict[str, Any], build).get("choice") if isinstance(build, dict) else None
    if choice == "login_limited":
        return ", on the login host under its own limits"
    if choice == "compute_job":
        return ", in an install job"
    return ""


def _where_built(report: Mapping[str, Any], *, site: str, job: str, last: str) -> str:
    """Say where JARVIS builds, from its build placement (``placement.build.choice``).

    Relay sets no placement: the site's answer to JARVIS's ``install_placement``
    question does, and JARVIS chooses the limits on the login host.
    """
    placement: object = report.get("placement")
    build = cast(dict[str, Any], placement).get("build") if isinstance(placement, dict) else None
    plan = cast(dict[str, Any], build) if isinstance(build, dict) else {}
    choice = plan.get("choice")
    if choice == "login_limited":
        return (
            f"JARVIS builds it on {site}'s login host under its own limits {last}, as a "
            "background task with no time limit that you can stop."
        )
    if choice is None and plan.get("questions"):
        return (
            f"Where JARVIS builds it, on the login host under its limits or as {job}, is "
            f"one of its questions; it installs it {last}."
        )
    return f"JARVIS installs it as {job} on the site {last}."

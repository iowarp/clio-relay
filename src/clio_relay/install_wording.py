"""The question relay asks before JARVIS installs a package's software on a site.

It is built from JARVIS's install report (``ppl install +check +json``): what is
reused, what is built, which choices JARVIS replays from the site's decisions
record, and how many questions follow.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from clio_relay.jarvis_submit import applied_decisions, choice_issues, install_summary


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
    built: list[str] = []
    for item in summary["build"]:
        detail = item["spec"] or item["package"]
        if item["missing"]:
            detail = f"{detail} ({item['missing']} missing)"
        built.append(detail)
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
    if not built and asked:
        return (
            f"JARVIS needs {asked} choice(s) from you {before} on {site}; nothing has to be "
            f"built. It applies your answers in a short job on the site, {then}.{reuse} Go ahead?"
        )
    if not built:
        return (
            f"JARVIS applies its recorded choices for {site} in a short job {before}; "
            f"nothing has to be built.{reuse} Go ahead?"
        )
    parts = [f"{package} needs software that {site} does not have yet."]
    if summary["reuse"]:
        parts.append("Reused: " + ", ".join(summary["reuse"]) + ".")
    if built:
        parts.append("Built: " + ", ".join(built) + ".")
    if asked:
        parts.append(f"JARVIS has {asked} question(s) for you after this.")
    if reuse:
        parts.append(reuse.strip())
    job = f"an install job of up to {walltime}" if walltime else "an install job"
    parts.append(f"JARVIS installs it as {job} on the site {last}. Install it?")
    return " ".join(parts)

"""JARVIS's install report, read for relay: what is missing, what to ask, what to refuse.

``jarvis ppl install +check`` and ``+submit`` print a ``jarvis.install-report.v1``
line; these functions turn it into the summary, the questions and the refusals
relay shows. They were split out of :mod:`clio_relay.jarvis_submit`, which builds
the commands.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any, cast

from clio_relay.jarvis_submit import INSTALL_REPORT_SCHEMA, NEEDS_INSTALL, parse_document
from clio_relay.jobs.refusals import Refusal, RefusalReason


def _dict(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def parse_install_report(text: str) -> dict[str, Any] | None:
    """Return the last ``jarvis.install-report.v1`` line in ``text``."""
    return parse_document(text, INSTALL_REPORT_SCHEMA)


def report_digest(report: Mapping[str, Any]) -> str:
    """Return the sha256 of an install report's installers and issues.

    Paths and the mode are left out, so the digest binds what the person approves.
    """
    bound = {"installers": report.get("installers"), "issues": report.get("issues")}
    text = json.dumps(bound, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode()).hexdigest()


def _issues(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    found = report.get("issues")
    if not isinstance(found, list):
        return []
    return [_dict(item) for item in cast(list[object], found) if _dict(item)]


def choice_issues(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the issues the person answers by picking one of JARVIS's candidates."""
    return [
        issue
        for issue in _issues(report)
        if issue.get("answerable")
        and isinstance(issue.get("candidates"), list)
        and issue["candidates"]
    ]


def blocking_issues(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the issues no answer fixes: no recipe, or answerable with no candidates.

    ``not_installed`` is not blocking: the install itself fixes it.
    """
    blocking: list[dict[str, Any]] = []
    for issue in _issues(report):
        if issue.get("kind") == "not_installed":
            continue
        if issue in choice_issues(report):
            continue
        blocking.append(issue)
    return blocking


REUSED_STATUSES = frozenset({"satisfied", "installed"})
"""JARVIS's package statuses for software the site already has."""


def install_summary(
    report: Mapping[str, Any], names: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Return what the install reuses and what it builds, per package.

    ``reuse`` names the packages the site already has; ``undecided`` the ones
    JARVIS cannot tell about until a question is answered (``needs_decision``);
    ``build`` lists the others, with the spec and how many of its dependencies are
    missing. JARVIS reports a package by its pipeline step (``pkg_name``);
    ``names`` (:func:`~clio_relay.jarvis_submit.package_names`) turns that back into the package.
    """
    names = names or {}
    reuse: list[str] = []
    undecided: list[str] = []
    build: list[dict[str, Any]] = []
    for method, installer in _dict(report.get("installers")).items():
        for step, package in _dict(_dict(installer).get("packages")).items():
            name = names.get(step, step)
            entry = _dict(package)
            if entry.get("status") in REUSED_STATUSES:
                reuse.append(name)
                continue
            if entry.get("status") == "needs_decision":
                undecided.append(name)
                continue
            missing = entry.get("missing")
            count = len(cast(list[object], missing)) if isinstance(missing, list) else None
            build.append(
                {
                    "package": name,
                    "method": method,
                    "status": entry.get("status"),
                    "spec": entry.get("spec"),
                    "missing": count,
                }
            )
    return {
        "reuse": sorted(reuse),
        "undecided": sorted(undecided),
        "build": build,
        "issues": len(_issues(report)),
    }


def software_missing(report: Mapping[str, Any]) -> bool:
    """Say whether JARVIS's report names software to build, not only questions to answer."""
    if install_summary(report)["build"]:
        return True
    return any(issue.get("kind") == "not_installed" for issue in _issues(report))


def software_installed(report: Mapping[str, Any], *, site: str) -> Refusal:
    """What a ``recheck`` that exited ``EXIT_INSTALLED`` raises: nothing to build.

    It is not a failure; :func:`is_installed` tells the caller to go ahead.
    """
    return Refusal(
        RefusalReason.SUBMIT_FAILED,
        f"the software is installed on {site} once JARVIS applies the answers; "
        "nothing has to be built",
        installed=True,
        install_report=dict(report),
    )


def is_installed(refusal: Refusal) -> bool:
    """Say whether ``refusal`` is :func:`software_installed`'s."""
    return refusal.data.get("installed") is True


def applied_decisions(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the decisions JARVIS applied from its own record, not from this round's answers.

    JARVIS replays what the person chose before (``source: decisions_file``)
    while it is still one of its candidates; relay keeps no copy of its own.
    """
    found = report.get("applied_decisions")
    listed = cast(list[Any], found) if isinstance(found, list) else []
    return [
        item
        for item in (_dict(raw) for raw in listed)
        if item.get("source") == "decisions_file" and item.get("constraint")
    ]


def choice_options(issue: Mapping[str, Any]) -> list[str]:
    """Return an issue's candidate constraints, in JARVIS's order."""
    options: list[str] = []
    for candidate in cast(list[object], issue.get("candidates") or []):
        constraint = _dict(candidate).get("constraint")
        if isinstance(constraint, str) and constraint and constraint not in options:
            options.append(constraint)
    return options


def _candidate_line(candidate: Mapping[str, Any]) -> str | None:
    constraint = candidate.get("constraint")
    if not isinstance(constraint, str) or not constraint:
        return None
    facts: list[str] = []
    source = candidate.get("source")
    if isinstance(source, str) and source:
        facts.append(source)
    modules = candidate.get("modules")
    if isinstance(modules, list) and modules:
        facts.append("modules " + ", ".join(str(item) for item in cast(list[object], modules)))
    version = candidate.get("version")
    if isinstance(version, str) and version:
        facts.append(version)
    prefix = candidate.get("prefix")
    if isinstance(prefix, str) and prefix:
        facts.append(prefix)
    implies = candidate.get("implies")
    if isinstance(implies, str) and implies:
        facts.append(implies)
    return f"{constraint} ({'; '.join(facts)})" if facts else constraint


def choice_message(issue: Mapping[str, Any]) -> str:
    """Return the question to ask for one of JARVIS's answerable issues.

    Each candidate is listed with where it comes from, its prefix and its modules
    (an ``ambiguous_provider`` MPI that is a site module carries ``modules``).
    """
    question = issue.get("question")
    subject = issue.get("package") or issue.get("subject") or "the package"
    text = (
        question
        if isinstance(question, str) and question
        else f"JARVIS needs a choice for {subject}: pick one"
    )
    lines = [
        line
        for candidate in cast(list[object], issue.get("candidates") or [])
        if (line := _candidate_line(_dict(candidate)))
    ]
    previous = _dict(issue.get("previous_answer"))
    if previous.get("constraint"):
        text = (
            f"{text} The earlier answer, {previous['constraint']}, no longer matches "
            "what JARVIS finds, so it asks again."
        )
    if issue.get("scope") == "site":
        text = f"{text} JARVIS records the answer for the site's later installs."
    return f"{text} Options: {'. '.join(lines)}." if lines else text


def install_needed(report: Mapping[str, Any], *, site: str, walltime: str | None = None) -> Refusal:
    """The refusal for a submit that exited 3: the software is missing or needs a choice.

    ``walltime`` is the install job's (``jarvis_submit.install_walltime``), for the ask.
    """
    summary = install_summary(report)
    message = (
        f"the package's software is not installed on {site}: JARVIS builds "
        f"{len(summary['build'])} package(s) once you approve"
    )
    if not summary["build"] and choice_issues(report):
        kinds = sorted({str(issue.get("kind")) for issue in choice_issues(report)})
        message = f"JARVIS needs a choice on {site} before the run ({', '.join(kinds)})"
    extra: dict[str, Any] = {"install_walltime": walltime} if walltime else {}
    return Refusal(
        RefusalReason.NEEDS_APPROVAL,
        message,
        needs=NEEDS_INSTALL,
        install_report=dict(report),
        summary=summary,
        **extra,
    )


def install_blocked(issues: list[dict[str, Any]], *, site: str) -> Refusal:
    """The refusal when JARVIS reports an install issue no answer can fix."""
    first = issues[0]
    detail = first.get("detail") or first.get("question") or first.get("kind")
    hint = first.get("hint")
    message = f"JARVIS cannot install the package's software on {site}: {detail}"
    if isinstance(hint, str) and hint:
        message = f"{message}. {hint}"
    return Refusal(RefusalReason.SUBMIT_FAILED, message, issues=issues, needs=NEEDS_INSTALL)


def answers_document(choices: Mapping[str, tuple[str, str]]) -> dict[str, Any]:
    """Return JARVIS's answers file from ``{issue_id: (constraint, ask_id)}``."""
    return {
        issue_id: {"constraint": constraint, "answered_by": "person", "ask_id": ask_id}
        for issue_id, (constraint, ask_id) in choices.items()
    }

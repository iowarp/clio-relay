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

from clio_relay.adapters.hooks import ChoiceOption
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

    ``not_installed`` is not blocking: the install itself fixes it. Nor is
    ``install_unchecked``: a check in a job (:func:`unchecked`) answers it.
    """
    blocking: list[dict[str, Any]] = []
    for issue in _issues(report):
        if issue.get("kind") in ("not_installed", UNCHECKED):
            continue
        if issue in choice_issues(report):
            continue
        blocking.append(issue)
    return blocking


UNCHECKED = "install_unchecked"
"""JARVIS's issue when the submitting host has no matching record of a job's check."""


def unchecked(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the ``install_unchecked`` issues: JARVIS must check in a job, nobody is asked.

    The light check on a scheduled pipeline's submitting host runs no Spack; it
    answers from ``install-check.json``, which only a check in a job writes.
    """
    return [issue for issue in _issues(report) if issue.get("kind") == UNCHECKED]


REUSED_STATUSES = frozenset({"satisfied", "installed"})
"""JARVIS's package statuses for software the site already has."""


def install_summary(
    report: Mapping[str, Any], names: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Return what the install reuses and what it installs, per package.

    ``reuse`` names the packages the site already has; ``undecided`` the ones
    JARVIS cannot tell about until a question is answered (``needs_decision``);
    ``build`` lists the others, with their install method, the spec (for pip,
    conda and container, the joined install query) and how many of its
    dependencies are missing. JARVIS reports a package by its pipeline step (``pkg_name``);
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
            spec = entry.get("spec")
            query = entry.get("query")
            if not spec and isinstance(query, list):
                spec = " ".join(str(item) for item in cast(list[object], query)) or None
            build.append(
                {
                    "package": name,
                    "method": method,
                    "status": entry.get("status"),
                    "spec": spec,
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


_SOURCES = {
    "site_modules": "the site's published Spack, whose modules the login host lists",
    "well_known": "found at a usual Spack location; the site does not publish it",
    "JARVIS_SITE_SPACKS": "listed in JARVIS_SITE_SPACKS",
    "resource_graph_site": "beside JARVIS's resource graph for this site",
    "JARVIS_SPACK_ROOT_sibling": "next to the Spack JARVIS_SPACK_ROOT names, which does not work",
    "SPACK_ROOT_sibling": "next to the Spack SPACK_ROOT names, which does not work",
    "PATH": "the one on PATH",
    "CONDA_EXE": "the one CONDA_EXE names",
    "MAMBA_EXE": "the one MAMBA_EXE names",
    "jarvis_miniforge": "the Miniforge JARVIS installed in the site home",
    "home": "found in your home directory",
}
"""What JARVIS's candidate ``source`` tags mean to a person (``discover_site_spacks``,
the conda and Python probes). A tag not listed here is shown as JARVIS wrote it."""


def _candidate_facts(candidate: Mapping[str, Any]) -> list[str]:
    facts: list[str] = []
    source = candidate.get("source")
    if isinstance(source, str) and source:
        facts.append(_SOURCES.get(source, source))
    modules = candidate.get("modules")
    if isinstance(modules, list) and modules:
        facts.append("modules " + ", ".join(str(item) for item in cast(list[object], modules)))
    version = candidate.get("version")
    if isinstance(version, str) and version:
        facts.append(f"version {version}" if version[:1].isdigit() else version)
    prefix = candidate.get("prefix")
    if isinstance(prefix, str) and prefix:
        facts.append(prefix)
    implies = candidate.get("implies")
    if isinstance(implies, str) and implies:
        facts.append(implies)
    does = candidate.get("does")
    if isinstance(does, str) and does:
        facts.append(does)
    return facts


MAX_EVIDENCE = 6
"""The most evidence lines of an issue (``install_placement``'s) a question repeats."""


def once(issue: Mapping[str, Any]) -> bool:
    """Say whether an issue's answer is for this occasion only (``ask_each_time``)."""
    subject = issue.get("subject")
    return issue.get("once") is True or (isinstance(subject, str) and subject.endswith(".occasion"))


def package_overrides(issue: Mapping[str, Any]) -> list[tuple[str, str]]:
    """Return an issue's ``(package, override issue_id)`` pairs, in JARVIS's order.

    ``install_placement`` offers one per package, so a person can place one
    package apart from the site's answer for the rest (a long build, say).
    """
    pairs: list[tuple[str, str]] = []
    for raw in cast(list[object], issue.get("package_overrides") or []):
        entry = _dict(raw)
        package, issue_id = entry.get("package"), entry.get("issue_id")
        if isinstance(package, str) and package and isinstance(issue_id, str) and issue_id:
            pairs.append((package, issue_id))
    return pairs


def override_option(constraint: str, package: str) -> str:
    """The option that answers ``constraint`` for one package only."""
    return f"{constraint} for {package} only"


def override_choices(issue: Mapping[str, Any]) -> dict[str, tuple[str, str]]:
    """Return ``{option: (override issue_id, constraint)}`` for an issue's per-package options."""
    if once(issue):
        return {}
    return {
        override_option(constraint, package): (issue_id, constraint)
        for package, issue_id in package_overrides(issue)
        for constraint in choice_options(issue)
    }


def choice_offers(issue: Mapping[str, Any]) -> list[str]:
    """Return the options to offer: the candidates, then the per-package ones."""
    return [*choice_options(issue), *override_choices(issue)]


PLACEMENT_KIND = "install_placement"

_TITLES = {
    "spack_missing": "Spack to use",
    "compiler_unavailable": "Compiler to use",
    "ambiguous_compiler": "Compiler to use",
    "ambiguous_provider": "MPI to use",
    "python_missing": "Python for pip packages",
    "conda_select": "Conda to use",
    "conda_missing": "Install conda",
    "container_engine": "Container engine",
    "install_method": "How to install",
    "variant_conflict": "Constraint to add",
    "concretize_failed": "How to resolve the Spack spec",
}

_LABELS = {
    "pip": "pip, in a Python venv",
    "conda": "conda",
    "spack": "Spack",
    "container": "Container image",
    "podman": "Podman",
    "apptainer": "Apptainer",
    "singularity": "Singularity",
    "docker": "Docker",
}


def _phase(issue: Mapping[str, Any]) -> str:
    phase = issue.get("phase")
    if isinstance(phase, str) and phase:
        return phase
    subject = str(issue.get("subject") or "")
    return "build" if ".build" in subject else "check"


def choice_title(issue: Mapping[str, Any]) -> str:
    """Return the human label of a JARVIS question's field."""
    kind = str(issue.get("kind") or "")
    package = issue.get("package")
    named = f" for {package}" if isinstance(package, str) and package not in ("", "*") else ""
    if kind == PLACEMENT_KIND:
        phase = _phase(issue)
        if once(issue):
            return f"Where JARVIS runs this Spack {phase}"
        return f"Where JARVIS runs Spack {phase}s at this site"
    found = _TITLES.get(kind)
    if found is not None:
        return found + (named if kind in ("ambiguous_provider", "install_method") else "")
    return (kind.replace("_", " ").capitalize() or "Choice") + named


def _placement_meaning(issue: Mapping[str, Any], constraint: str) -> tuple[str, str] | None:
    """Return the label and one-line meaning of an ``install_placement`` option."""
    phase = _phase(issue)
    if constraint == "login_limited":
        host = _dict(issue.get("login_host")).get("host")
        limits = _dict(issue.get("limits"))
        bounds = [
            f"nice {limits['nice']}" if limits.get("nice") is not None else "",
            f"{limits['jobs']} job(s)" if limits.get("jobs") is not None else "",
            f"up to {limits['memory_mb']} MB" if limits.get("memory_mb") is not None else "",
        ]
        bounded = ", ".join(item for item in bounds if item)
        busy = cast(list[object], limits.get("defer_reasons") or [])
        text = (
            f"Runs on the login host{f' {host}' if host else ''} right away"
            f"{f', bounded ({bounded})' if bounded else ', bounded'}: no queue wait and no "
            "allocation charge, but it shares the login host with everyone on it"
        )
        if limits.get("defer") and busy:
            text += "; it is busy now (" + "; ".join(str(item) for item in busy) + ")"
        return "Login host, now", text + "."
    if constraint == "compute_job":
        queue = _dict(issue.get("queue"))
        wait = (
            f"about {queue['wait']} in the queue now"
            if queue.get("measured") and queue.get("wait")
            else "a queue wait relay cannot measure here"
        )
        return (
            "Compute node job",
            f"Runs the {phase} in a short 1-node batch job: {wait} and a little allocation "
            "time, and it keeps the login host light.",
        )
    if constraint == "ask_each_time":
        return (
            "Ask each time",
            f"Records no standing answer: JARVIS asks before every Spack {phase} here.",
        )
    return None


def _label(candidate: Mapping[str, Any], constraint: str) -> str:
    for key in ("label", "summary"):
        found = candidate.get(key)
        if isinstance(found, str) and found:
            return found[:1].upper() + found[1:]
    return _LABELS.get(constraint, constraint)


def choice_details(issue: Mapping[str, Any]) -> list[ChoiceOption]:
    """Return a JARVIS question's options typed: label, one-line meaning, recommended.

    The values are :func:`choice_offers`'s, so an answer reads as before. The
    recommended option is the one JARVIS recommends (the issue's ``recommended``,
    else a candidate's own flag); relay recommends none of its own.
    """
    placement = issue.get("kind") == PLACEMENT_KIND
    pick = issue.get("recommended")
    typed: dict[str, ChoiceOption] = {}
    for raw in cast(list[object], issue.get("candidates") or []):
        candidate = _dict(raw)
        constraint = candidate.get("constraint")
        if not isinstance(constraint, str) or not constraint or constraint in typed:
            continue
        meaning = _placement_meaning(issue, constraint) if placement else None
        if meaning is None:
            facts = _candidate_facts(candidate)
            text = "; ".join(facts)
            meaning = (
                _label(candidate, constraint),
                text[:1].upper() + text[1:] + "." if text else "",
            )
        recommended = (
            constraint == pick if isinstance(pick, str) else candidate.get("recommended") is True
        )
        typed[constraint] = ChoiceOption(
            value=constraint,
            label=meaning[0],
            description=meaning[1] or None,
            recommended=recommended,
        )
    for option, (_issue_id, constraint) in override_choices(issue).items():
        base = typed.get(constraint)
        package = option.removeprefix(f"{constraint} for ").removesuffix(" only")
        typed[option] = ChoiceOption(
            value=option,
            label=f"{base.label if base else constraint} for {package} only",
            description=(
                f"Places only {package} this way; JARVIS asks again for the other packages."
            ),
        )
    return list(typed.values())


def recommended_reason(issue: Mapping[str, Any]) -> str | None:
    """Return why JARVIS recommends its pick, when it says."""
    found = issue.get("recommendation_reason")
    return found if isinstance(found, str) and found else None


def recommendation_basis(issue: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return what JARVIS's pick rests on, as JARVIS wrote it, when it says.

    jarvis-cd d04b29e and later: a ``source`` (``site_decision``,
    ``resource_graph``, ``heuristic``) and, for a recorded answer, who gave it,
    when and why.
    """
    found = issue.get("recommendation_basis")
    return dict(cast(dict[str, Any], found)) if isinstance(found, dict) and found else None


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
    details = choice_details(issue)
    own = set(choice_options(issue))
    pick = next((item for item in details if item.recommended), None)
    if pick is not None and pick.label != pick.value:
        text = text.replace(f"Recommended: {pick.value}", f"Recommended: {pick.label}")
    lines = [
        f"{item.label}{' (recommended)' if item.recommended else ''}"
        + (f": {item.description.rstrip('.')}" if item.description else "")
        for item in details
        if item.value in own
    ]
    previous = _dict(issue.get("previous_answer"))
    if previous.get("constraint"):
        text = (
            f"{text} The earlier answer, {previous['constraint']}, no longer matches "
            "what JARVIS finds, so it asks again."
        )
    if once(issue):
        text = f"{text} The answer is for this run only; JARVIS asks again next time."
    elif issue.get("scope") == "site":
        text = f"{text} JARVIS records the answer for the site's later installs."
    overrides = package_overrides(issue)
    if overrides and not once(issue):
        named = ", ".join(package for package, _ in overrides)
        text = (
            f"{text} An option ending in 'for <package> only' places that package alone "
            f"({named}), and JARVIS asks again for the rest."
        )
    text = f"{text} Options: {'. '.join(lines)}." if lines else text
    evidence = [
        str(item)
        for item in cast(list[object], issue.get("evidence") or [])[:MAX_EVIDENCE]
        if isinstance(item, str) and item
    ]
    return f"{text} Evidence: {'; '.join(evidence)}." if evidence else text


def install_needed(report: Mapping[str, Any], *, site: str, walltime: str | None = None) -> Refusal:
    """The refusal for a submit that exited 3: the software is missing or needs a choice.

    ``walltime`` is the install job's (``jarvis_submit.install_walltime``), for the ask.
    """
    summary = install_summary(report)
    message = (
        f"the package's software is not installed on {site}: JARVIS installs "
        f"{len(summary['build'])} package(s) once you approve"
    )
    if not summary["build"] and choice_issues(report):
        kinds = sorted({str(issue.get("kind")) for issue in choice_issues(report)})
        message = f"JARVIS needs a choice on {site} before the run ({', '.join(kinds)})"
    elif not summary["build"] and not software_missing(report) and unchecked(report):
        message = (
            f"JARVIS has no check of the package's software on {site} yet; "
            "it checks in a job before the run"
        )
    extra: dict[str, Any] = {"install_walltime": walltime} if walltime else {}
    return Refusal(
        RefusalReason.NEEDS_APPROVAL,
        message,
        needs=NEEDS_INSTALL,
        install_report=dict(report),
        summary=summary,
        **extra,
    )


def check_unrecorded(issues: list[dict[str, Any]], *, site: str) -> Refusal:
    """The refusal when a check job ended and the login host still finds no record of it.

    JARVIS's ``install_unchecked`` says why (``inputs_changed``, ``answers_pending``,
    ...); relay does not loop on another job.
    """
    first = issues[0]
    reason = first.get("reason") or first.get("detail") or "no matching record"
    hint = first.get("hint")
    message = (
        f"JARVIS checked the package's software in a job on {site}, but the check on the "
        f"login host still finds no matching record of it ({reason})"
    )
    if isinstance(hint, str) and hint:
        message = f"{message}. {hint}"
    return Refusal(
        RefusalReason.SUBMIT_FAILED,
        message,
        issues=issues,
        record=first.get("record"),
        needs=NEEDS_INSTALL,
    )


def install_blocked(issues: list[dict[str, Any]], *, site: str) -> Refusal:
    """The refusal when JARVIS reports an install issue no answer can fix."""
    first = issues[0]
    detail = first.get("detail") or first.get("question") or first.get("kind")
    hint = first.get("hint")
    message = f"JARVIS cannot install the package's software on {site}: {detail}"
    if isinstance(hint, str) and hint:
        message = then(message, hint)
    return Refusal(RefusalReason.SUBMIT_FAILED, message, issues=issues, needs=NEEDS_INSTALL)


def then(sentence: str, more: str) -> str:
    """``sentence``, ended once, followed by ``more``."""
    sentence = sentence.rstrip()
    return f"{sentence} {more}" if sentence.endswith((".", "!", "?")) else f"{sentence}. {more}"


def answers_document(choices: Mapping[str, tuple[str, str]]) -> dict[str, Any]:
    """Return JARVIS's answers file from ``{issue_id: (constraint, ask_id)}``."""
    return {
        issue_id: {"constraint": constraint, "answered_by": "person", "ask_id": ask_id}
        for issue_id, (constraint, ask_id) in choices.items()
    }

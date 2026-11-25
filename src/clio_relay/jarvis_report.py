"""JARVIS's install report, read for relay: what is missing, what to ask, what to refuse.

``jarvis ppl install +check`` and ``+submit`` print a ``jarvis.install-report.v1``
line; these functions turn it into the summary, the questions and the refusals
relay shows. They were split out of :mod:`clio_relay.jarvis_submit`, which builds
the commands.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any, cast

from clio_relay.adapters.hooks import ChoiceOption, split_question
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
        for step, entry in _installer_packages(_dict(installer)):
            name = names.get(step, step)
            if entry.get("status") in REUSED_STATUSES:
                reuse.append(name)
                continue
            if entry.get("status") == "needs_decision":
                undecided.append(name)
                continue
            missing = entry.get("missing")
            count = len(cast(list[object], missing)) if isinstance(missing, list) else None
            spec = entry.get("spec") or entry.get("image")
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


def build_facts(
    report: Mapping[str, Any], names: Mapping[str, str] | None = None
) -> list[dict[str, Any]]:
    """Return what JARVIS's check found for each package it builds or installs.

    One entry per :func:`install_summary` build item, with what JARVIS reported
    beside it: for Spack, how many of the spec's packages the site already has
    (``present`` of ``total``), the MPI it builds against (``mpi``: name,
    version, prefix, modules), and JARVIS's build estimate from its build
    placement (``placement.build.packages[step].estimate``). A fact JARVIS did
    not report is ``None``.
    """
    names = names or {}
    placement = _dict(report.get("placement"))
    planned = _dict(_dict(placement.get("build")).get("packages"))
    found: list[dict[str, Any]] = []
    for method, installer in _dict(report.get("installers")).items():
        for step, entry in _installer_packages(_dict(installer)):
            if entry.get("status") in REUSED_STATUSES or entry.get("status") == "needs_decision":
                continue
            counts = _dict(entry.get("nodes_by_status"))
            total = entry.get("nodes_total")
            missing = counts.get("missing", 0) if counts else None
            present = (
                total - missing
                if isinstance(total, int) and isinstance(missing, int) and total >= missing
                else None
            )
            mpi = entry.get("mpi")
            estimate = _dict(_dict(planned.get(step)).get("estimate"))
            human = estimate.get("human")
            spec = entry.get("spec") or entry.get("image")
            query = entry.get("query")
            if not spec and isinstance(query, list):
                spec = " ".join(str(item) for item in cast(list[object], query)) or None
            found.append(
                {
                    "package": names.get(step, step),
                    "method": method,
                    "spec": spec,
                    "total": total if isinstance(total, int) else None,
                    "present": present,
                    "mpi": cast(dict[str, Any], mpi) if isinstance(mpi, dict) else None,
                    "estimate": human if isinstance(human, str) and human != "unknown" else None,
                }
            )
    return found


def _installer_packages(installer: Mapping[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Return ``(step, state)`` per package an installer reports.

    Spack, pip and conda give each step its own state. The container installer
    lists its steps and keeps one state (``status``, ``image``) for the image they share.
    """
    packages = installer.get("packages")
    if isinstance(packages, list):
        shared = {key: value for key, value in installer.items() if key != "packages"}
        return [(str(step), shared) for step in cast(list[object], packages) if step]
    return [(str(step), _dict(state)) for step, state in _dict(packages).items()]


def software_missing(report: Mapping[str, Any]) -> bool:
    """Say whether JARVIS's report names software to build, not only questions to answer."""
    if install_summary(report)["build"]:
        return True
    return any(issue.get("kind") == "not_installed" for issue in _issues(report))


def _missing_key(method: object, package: object, subject: object) -> str:
    return f"{method}:{package}:{subject}"


def missing_keys(report: Mapping[str, Any]) -> list[str]:
    """Return what the report says is not installed, as ``method:package:subject`` keys.

    One key per ``not_installed`` issue and per package to build, so a later
    report can be compared with this one.
    """
    keys = {
        _missing_key(issue.get("method"), issue.get("package"), issue.get("subject"))
        for issue in _issues(report)
        if issue.get("kind") == "not_installed"
    }
    keys.update(
        _missing_key(entry["method"], entry["package"], entry["spec"])
        for entry in install_summary(report)["build"]
    )
    return sorted(keys)


STILL_MISSING = "installed_still_missing"
"""Relay's kind when an install job succeeded and JARVIS's check still finds it missing."""


def still_missing(report: Mapping[str, Any], repeated: Iterable[str], *, site: str) -> Refusal:
    """The refusal when JARVIS's check reports missing what an install job just installed.

    Asking the same approval again would loop the person on a JARVIS fault, so
    the operation ends with JARVIS's own subject and message.
    """
    repeated = sorted(set(repeated))
    issues = [
        issue
        for issue in _issues(report)
        if issue.get("kind") == "not_installed"
        and _missing_key(issue.get("method"), issue.get("package"), issue.get("subject"))
        in repeated
    ]
    first = issues[0] if issues else {}
    subject = first.get("subject") or ", ".join(key.split(":", 2)[2] for key in repeated)
    message = (
        f"JARVIS installed the package's software on {site}, but its check still finds "
        f"it missing: {subject}"
    )
    detail = first.get("question") or first.get("detail")
    if isinstance(detail, str) and detail:
        message = then(message, detail)
    return Refusal(
        RefusalReason.SUBMIT_FAILED,
        message,
        kind=STILL_MISSING,
        subject=subject,
        still_missing=repeated,
        issues=issues,
        needs=NEEDS_INSTALL,
        retry=False,
    )


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
SITE_COPY_KIND = "site_copy"
"""JARVIS's question (jarvis-cd 1.9.7, ``jarvis.site-copies.v1``): use a copy the site
already has of the package (a module, a site Spack install), or build it with Spack."""
SITE_COPY_NONE = "site_copy_none"
"""JARVIS's fact that its search found no site copy, so Spack builds the package."""
SITE_COPY_UNUSABLE = "site_copy_unusable"
"""JARVIS's question (jarvis-cd 1.9.8) when the site copy chosen for a package does not
load: another copy, a Spack build, or forget the choice and be asked again."""
SPACK_BROKEN = "spack_broken"
"""JARVIS's question (jarvis-cd 1.9.8) when a Spack fails before any package: run its
commands without the person's user config, skip that Spack, or check again."""


def site_copy_facts(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return JARVIS's ``site_copy_none`` facts: the packages it found no site copy of.

    They are in the report's ``site_copies.facts``, not its issues: nothing is asked.
    """
    found = _dict(report.get("site_copies")).get("facts")
    listed = cast(list[object], found) if isinstance(found, list) else []
    return [item for item in map(_dict, listed) if item.get("kind") == SITE_COPY_NONE]


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
    SITE_COPY_KIND: "Site copy or Spack build",
    SITE_COPY_UNUSABLE: "Site copy that does not load",
    SPACK_BROKEN: "A Spack that fails",
}
_NAMED_TITLES = frozenset(
    {"ambiguous_provider", "install_method", SITE_COPY_KIND, SITE_COPY_UNUSABLE}
)

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
        return found + (named if kind in _NAMED_TITLES else "")
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


def _label(candidate: Mapping[str, Any], constraint: str, *, as_written: bool = False) -> str:
    for key in ("label", "summary"):
        found = candidate.get(key)
        if isinstance(found, str) and found:
            return found if as_written else found[:1].upper() + found[1:]
    return _LABELS.get(constraint, constraint)


def _sentence(text: str) -> str:
    text = text.strip()
    if not text:
        return ""
    text = text[:1].upper() + text[1:]
    return text if text.endswith((".", "!", "?")) else text + "."


def _written(candidate: Mapping[str, Any]) -> bool:
    """Say whether JARVIS wrote what a candidate does (``site_copy``, ``spack_broken``...)."""
    does = candidate.get("does")
    return isinstance(does, str) and bool(does.strip())


def _written_meaning(candidate: Mapping[str, Any]) -> str:
    """What picking a candidate does, why, and what it leaves out: JARVIS's words."""
    parts = [candidate.get("does"), candidate.get("why"), candidate.get("leaves_out")]
    return " ".join(_sentence(part) for part in parts if isinstance(part, str) and part.strip())


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
        if meaning is None and _written(candidate):
            meaning = (
                _label(candidate, constraint, as_written=True),
                _written_meaning(candidate),
            )
        if meaning is None:
            facts = _candidate_facts(candidate)
            text = "; ".join(facts)
            meaning = (
                _label(candidate, constraint),
                text[:1].upper() + text[1:] + "." if text else "",
            )
        # JARVIS names its pick by constraint, or (a site copy) by the candidate's id.
        recommended = (
            pick in (constraint, candidate.get("id"))
            if isinstance(pick, str)
            else candidate.get("recommended") is True
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


def _split_question(issue: Mapping[str, Any]) -> tuple[str, str]:
    """Return JARVIS's question sentence and the rest of its text."""
    question = issue.get("question")
    package = issue.get("package")
    subject = (
        package
        if isinstance(package, str) and package not in ("", "*")
        else issue.get("subject") or "this install"
    )
    if not isinstance(question, str) or not question.strip():
        return f"Which option should JARVIS use for {subject}?", ""
    return split_question(question)


def choice_message(issue: Mapping[str, Any]) -> str:
    """Return the short question to ask for one of JARVIS's answerable issues.

    Only the question: each option's meaning is in its own description
    (:func:`choice_details`), what the answer covers in :func:`choice_note`, and
    JARVIS's other text and evidence in :func:`choice_detail` and
    :func:`choice_evidence`.
    """
    return _split_question(issue)[0]


def choice_note(issue: Mapping[str, Any]) -> str | None:
    """Return what an answer covers, and why JARVIS asks again when it does."""
    notes: list[str] = []
    previous: object = issue.get("previous_answer")
    earlier = (
        cast(dict[str, Any], previous).get("constraint") if isinstance(previous, dict) else previous
    )
    if isinstance(earlier, str) and earlier:
        notes.append(
            f"The earlier answer, {earlier}, no longer matches what JARVIS finds, so it asks again."
        )
    if once(issue):
        notes.append("The answer is for this run only; JARVIS asks again next time.")
    elif issue.get("kind") == SPACK_BROKEN:
        notes.append(
            "JARVIS records a choice to go on without the user config or to skip this Spack "
            "for the site's later installs; checking again records nothing."
        )
    elif issue.get("scope") == "site":
        notes.append("JARVIS records the answer for the site's later installs.")
    elif issue.get("kind") == SITE_COPY_KIND and issue.get("package"):
        notes.append(
            f"JARVIS records the answer for {issue['package']} on this site, and later "
            "installs use it without searching again."
        )
    elif issue.get("kind") == SITE_COPY_UNUSABLE and issue.get("package"):
        notes.append(
            f"Another copy or the Spack build replaces the site copy JARVIS recorded for "
            f"{issue['package']} on this site."
        )
    return " ".join(notes) or None


def choice_detail(issue: Mapping[str, Any]) -> str | None:
    """Return JARVIS's text past its question, with option values read as their labels."""
    rest = _split_question(issue)[1]
    if not rest:
        return None
    pick = next((item for item in choice_details(issue) if item.recommended), None)
    if pick is not None and pick.label != pick.value:
        rest = rest.replace(f"Recommended: {pick.value}", f"Recommended: {pick.label}")
    return rest


def choice_evidence(issue: Mapping[str, Any]) -> list[str]:
    """Return JARVIS's evidence for a question, as it wrote it."""
    return [
        item for item in cast(list[object], issue.get("evidence") or []) if isinstance(item, str)
    ]


def install_needed(report: Mapping[str, Any], *, site: str, walltime: str | None = None) -> Refusal:
    """The refusal for a submit that exited 3: the software is missing or needs a choice.

    ``walltime`` is the install job's (``jarvis_submit.install_walltime``), for the ask.
    """
    summary = install_summary(report)
    found = ", ".join(str(item["spec"] or item["package"]) for item in summary["build"])
    message = (
        f"JARVIS's check finds no installed {found or 'copy of the software'} on {site}; "
        f"JARVIS installs {len(summary['build'])} package(s) once the person approves, "
        "and relay asks them"
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

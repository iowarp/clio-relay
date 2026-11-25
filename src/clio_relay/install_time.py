"""JARVIS's ``install_time_exceeds_partition`` refusal, asked as a choice.

JARVIS sizes an install job's walltime from its own estimate of the build
(jarvis-cd d11675b and later). When that estimate is longer than the
partition's ``MaxTime``, ``ppl install +submit`` exits 3 with a
``jarvis.refusal.v1`` whose issue offers candidates: a partition whose
``MaxTime`` fits (``set: {scheduler.install.partition: ...}``), submitting up
to ``MaxTime`` and building in parts (``set: {scheduler.install.time: ...}``),
or refreshing the check so the estimate counts only what is missing
(``command``). JARVIS does not take an answer for it: the fix is the install
pipeline's ``scheduler.install``.

Relay asks the person to pick one, as it asks JARVIS's other questions
(:func:`question` is the issue in that shape). A ``set`` pick reaches the
install pipeline as :data:`~clio_relay.jarvis_submit.INSTALL_SCHEDULER`
(:func:`~clio_relay.jarvis_submit.install_pipeline`); the check pick runs a
check job (``recheck``). Relay adds no partition or time of its own.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from clio_relay import jarvis_submit
from clio_relay.jobs.refusals import Refusal, RefusalReason

KIND = "install_time_exceeds_partition"
SCHEDULER_PREFIX = "scheduler.install."
CHECK_AGAIN = "check_again"
"""The option that refreshes JARVIS's check (its ``command`` candidate)."""


def _dict(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def exceeds_issue(text: str) -> dict[str, Any] | None:
    """Return the ``install_time_exceeds_partition`` issue in JARVIS's output, if any."""
    document = jarvis_submit.refusal_document(text)
    for issue in jarvis_submit.typed_issues(document):
        if issue.get("kind") == KIND:
            return issue
    return None


def _human(seconds: object) -> str | None:
    if not isinstance(seconds, int | float) or isinstance(seconds, bool) or seconds <= 0:
        return None
    minutes = int(-(-seconds // 60))
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    parts = [f"{days} d" if days else "", f"{hours} h" if hours else "", f"{minutes} min"]
    return " ".join(part for part in parts if part and part != "0 min") or "1 min"


def _option(candidate: Mapping[str, Any]) -> tuple[str, str] | None:
    """Return an option's value and label for one of JARVIS's candidates, or None."""
    settings = _dict(candidate.get("set"))
    if settings:
        if not all(key.startswith(SCHEDULER_PREFIX) for key in settings):
            return None
        value = ",".join(
            f"{key.removeprefix(SCHEDULER_PREFIX)}={settings[key]}" for key in sorted(settings)
        )
        partition = settings.get(f"{SCHEDULER_PREFIX}partition")
        time = settings.get(f"{SCHEDULER_PREFIX}time")
        if partition and len(settings) == 1:
            return value, f"Build in partition {partition}"
        if time and len(settings) == 1:
            return value, f"Submit up to {time}, in parts"
        return value, "Set " + ", ".join(f"{key} {settings[key]}" for key in sorted(settings))
    if candidate.get("command"):
        return CHECK_AGAIN, "Check again first"
    return None


def question(issue: Mapping[str, Any]) -> dict[str, Any]:
    """Return the issue in the shape of JARVIS's answerable questions, for one choice ask.

    Each candidate becomes an option (``constraint``) with its label and
    JARVIS's own ``does``; JARVIS's message, remedy and estimate go in the
    question's detail and evidence.
    """
    details = _dict(issue.get("details"))
    identity = _dict(issue.get("identity"))
    partition = details.get("partition") or identity.get("partition") or "the partition"
    needed = _human(details.get("needed_seconds"))
    limit = details.get("max_time")
    sentence = (
        f"The build needs about {needed}, longer than {partition} allows"
        f"{f' ({limit})' if limit else ''}: how should JARVIS run it?"
        if needed
        else f"The build needs longer than {partition} allows: how should JARVIS run it?"
    )
    said = [str(issue[key]) for key in ("message", "remedy") if issue.get(key)]
    candidates: list[dict[str, Any]] = []
    for raw in cast(list[object], issue.get("candidates") or []):
        candidate = _dict(raw)
        option = _option(candidate)
        if option is None or any(item["constraint"] == option[0] for item in candidates):
            continue
        candidates.append(
            {"constraint": option[0], "label": option[1], "does": candidate.get("does")}
        )
    evidence: list[str] = []
    estimate = _dict(details.get("estimate"))
    for key in ("formula", "human", "why"):
        if isinstance(estimate.get(key), str) and estimate[key]:
            evidence.append(f"estimate: {estimate[key]}")
            break
    if details.get("already_built"):
        evidence.append(
            f"Spack nodes already built by earlier attempts: {details['already_built']}"
        )
    return {
        **dict(issue),
        "answerable": True,
        "question": " ".join([sentence, *said]),
        "candidates": candidates,
        "evidence": evidence,
    }


def refusal(issue: Mapping[str, Any], *, site: str) -> Refusal:
    """The refusal that turns the issue into the person's choice (an install ask)."""
    asked = question(issue)
    details = _dict(issue.get("details"))
    needed = _human(details.get("needed_seconds"))
    partition = details.get("partition") or "its partition"
    message = (
        f"JARVIS's install job on {site} needs "
        f"{f'about {needed}' if needed else 'longer'}, more than {partition} allows; "
        "it does not cut a build short, so the person chooses how it runs"
    )
    return Refusal(
        RefusalReason.NEEDS_APPROVAL,
        message,
        kind=KIND,
        needs=jarvis_submit.NEEDS_INSTALL,
        install_report={"issues": [asked]},
        issue_id=asked.get("issue_id"),
        issues=[dict(issue)],
    )


def picked(value: str) -> dict[str, str] | None:
    """Return the ``scheduler.install`` settings an option sets; None for :data:`CHECK_AGAIN`."""
    if value == CHECK_AGAIN:
        return None
    settings: dict[str, str] = {}
    for part in value.split(","):
        key, found, setting = part.partition("=")
        if found and key and setting:
            settings[key] = setting
    return settings

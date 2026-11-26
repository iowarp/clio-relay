"""JARVIS's install failure records (exit 6), passed on whole.

An install, check or submit that fails exits 6 (jarvis-cd 1.9.12) and prints its
install report (``jarvis.install-report.v1``) with ``failed: true`` and
``failures``, the ids of its failure records among the report's ``issues``. A
record (``failure_schema: jarvis.failure.v1``) says what JARVIS was doing
(``stage``, ``doing``), where (``host``, ``cwd``), the ``command`` and its
``exit_code``, an ``output_tail`` (a preview), ``output_path`` (the whole output
on the site, for ``files``) and the ``paths`` involved (``install_log``, a build
log). An install job writes the same records to
``<execution>/install/issues.jsonl``.

Relay asks nothing about a failure and adds no words of its own: the refusal's
message is JARVIS's, and its data holds the records and the report whole, so
the agent investigates and acts through the usual verbs. Only an exit 3 is a
decision, which becomes the install asks.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any, cast

from clio_relay import jarvis_report, jarvis_submit, site_facts
from clio_relay.jobs.refusals import Refusal, RefusalReason

EXIT_INSTALL_FAILED = 6
"""``ppl install`` or ``ppl submit`` exit: something JARVIS ran failed; see its records."""

TAIL_LINES = 40
"""How much of the output relay passes when JARVIS printed no record at all."""


def _dicts(items: object) -> list[dict[str, Any]]:
    listed = cast(list[object], items) if isinstance(items, list) else []
    return [cast(dict[str, Any], item) for item in listed if isinstance(item, dict) and item]


def read_jsonl(text: str) -> list[dict[str, Any]]:
    """Return the issues of an ``issues.jsonl``, skipping lines that are not JSON objects."""
    issues: list[dict[str, Any]] = []
    for line in text.splitlines():
        try:
            found: object = json.loads(line)
        except ValueError:
            continue
        issues.extend(_dicts([found]))
    return issues


def records(
    issues: Iterable[Mapping[str, Any]], named: Iterable[object] = ()
) -> list[dict[str, Any]]:
    """Return the failure records among ``issues``, one per ``issue_id`` (the last wins).

    ``named`` is the report's ``failures``. Without it (an install job's
    ``issues.jsonl``), the records are the issues JARVIS says no answer settles
    (``answerable`` false).
    """
    ids = {str(item) for item in named}
    found: dict[str, dict[str, Any]] = {}
    for issue in issues:
        if (str(issue.get("issue_id")) in ids) if ids else not issue.get("answerable"):
            found[str(issue.get("issue_id") or len(found))] = dict(issue)
    return list(found.values())


def _tail(text: str) -> str:
    return "\n".join(text.strip().splitlines()[-TAIL_LINES:])


def failed(
    issues: list[dict[str, Any]],
    *,
    site: str,
    output: str = "",
    named: Iterable[object] = (),
    exit_code: object = EXIT_INSTALL_FAILED,
    **extra: Any,
) -> Refusal:
    """The refusal for an install JARVIS reports failed: its records and words, whole.

    The message is each record's own line (:func:`clio_relay.jarvis_report.words`);
    with no record it is the end of what JARVIS printed (``output``).
    """
    found = records(issues, named)
    said = [line for line in map(jarvis_report.words, found) if line]
    message = "\n".join(said) or _tail(output) or f"JARVIS exited {exit_code} with no output"
    return Refusal(
        RefusalReason.SUBMIT_FAILED,
        message,
        site=site,
        failures=found,
        issues=issues,
        exit_code=exit_code,
        retry=False,
        **extra,
    )


def refusal(text: str, *, site: str) -> Refusal:
    """The refusal for a ``ppl install`` or ``ppl submit`` that exited 6."""
    report = jarvis_report.parse_install_report(text) or {}
    extra = {key: report[key] for key in ("issues_path", "record_path") if report.get(key)}
    if report:
        extra["report"] = report
    return failed(
        _dicts(report.get("issues")),
        site=site,
        output=text,
        named=cast(list[object], report.get("failures") or []),
        **extra,
    )


def raise_for(exit_code: object, text: str, *, site: str) -> None:
    """Raise JARVIS's exits relay passes on before its own handling.

    Exit 6 is a failure (:func:`refusal`); exit 3 with a blocking site-fact
    question is :func:`clio_relay.site_facts.refusal`. Any other exit 3 is an
    install report, which the caller turns into the install asks.
    """
    if str(exit_code) == str(jarvis_submit.EXIT_DECISION):
        site_facts.raise_for(text, site=site)
    if str(exit_code) == str(EXIT_INSTALL_FAILED):
        raise refusal(text, site=site)

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
from typing import TYPE_CHECKING, Any, cast

from clio_relay import jarvis_report, jarvis_submit, site_facts
from clio_relay.jobs.refusals import Refusal, RefusalReason

if TYPE_CHECKING:
    from clio_relay.jobs.records import OperationRecord
    from clio_relay.jobs.spec import JobSpec

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


FAILURE_KIND = "failure"
"""JARVIS's kind for a failure record (``failure_schema: jarvis.failure.v1``)."""


def is_record(issue: Mapping[str, Any]) -> bool:
    """Say whether an issue is one of JARVIS's failure records.

    JARVIS's contract: ``kind: failure``, or a ``compiler_unavailable`` no answer
    settles (the compiler's executables are missing and no usable one is left).
    A check's other unanswerable issues (``not_installed``, ``install_unchecked``)
    are not failures.
    """
    kind = issue.get("kind")
    return kind == FAILURE_KIND or (kind == "compiler_unavailable" and not issue.get("answerable"))


def records(
    issues: Iterable[Mapping[str, Any]], named: Iterable[object] = ()
) -> list[dict[str, Any]]:
    """Return the failure records among ``issues``, one per ``issue_id`` (the last wins).

    ``named`` is the report's ``failures``. Without it (an install job's
    ``issues.jsonl``), the records are :func:`is_record`'s.
    """
    ids = {str(item) for item in named}
    found: dict[str, dict[str, Any]] = {}
    for issue in issues:
        if (str(issue.get("issue_id")) in ids) if ids else is_record(issue):
            found[str(issue.get("issue_id") or len(found))] = dict(issue)
    return list(found.values())


def _tail(text: str) -> str:
    return "\n".join(text.strip().splitlines()[-TAIL_LINES:])


SETTINGS_KEY = "settings"
"""A failure record's install settings (jarvis-cd 1.9.14): what the person may set."""

SETTINGS_HOW = (
    "To give a setting, run the same job again with spec.install_settings, or call install "
    "again with install_settings, as [{issue_id, value, basis}]"
)


def offered(found: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return the install settings the records offer, whole, one per ``issue_id``.

    JARVIS puts them on a failure record as the settings the person can change,
    not as a reading of the error; relay passes them on as they are.
    """
    settings: dict[str, dict[str, Any]] = {}
    for record in found:
        for setting in _dicts(record.get(SETTINGS_KEY)):
            if isinstance(setting.get("issue_id"), str):
                settings.setdefault(str(setting["issue_id"]), setting)
    return list(settings.values())


def _offer_line(setting: Mapping[str, Any]) -> str:
    question = str(setting.get("question") or setting.get("setting") or "").strip()
    answer = str(setting.get("answer") or "").strip()
    return f"{question} ({answer})" if question and answer else question


def outcome(
    issues: list[dict[str, Any]],
    *,
    output: str = "",
    named: Iterable[object] = (),
    exit_code: object = EXIT_INSTALL_FAILED,
) -> tuple[str, dict[str, Any]]:
    """JARVIS's words for an install it reports failed, and its records, whole.

    The message is each record's own line (:func:`clio_relay.jarvis_report.words`);
    with no record it is the end of what JARVIS printed (``output``). The install
    settings the records offer (:func:`offered`) follow in JARVIS's words, and
    travel whole as ``settings``. The data is ``failures`` and ``issues``, with
    ``settings`` and ``settings_how`` when there are settings.
    """
    found = records(issues, named)
    said = [line for line in map(jarvis_report.words, found) if line]
    message = "\n".join(said) or _tail(output) or f"JARVIS exited {exit_code} with no output"
    data: dict[str, Any] = {"failures": found, "issues": issues}
    settings = offered(found)
    if settings:
        lines = [line for line in map(_offer_line, settings) if line]
        message = "\n".join([message, *lines])
        data.update({SETTINGS_KEY: settings, "settings_how": SETTINGS_HOW})
    return message, data


def failed(
    issues: list[dict[str, Any]],
    *,
    site: str,
    output: str = "",
    named: Iterable[object] = (),
    exit_code: object = EXIT_INSTALL_FAILED,
    **extra: Any,
) -> Refusal:
    """The refusal for a submit whose ``ppl install`` JARVIS reports failed (:func:`outcome`)."""
    message, data = outcome(issues, output=output, named=named, exit_code=exit_code)
    return Refusal(
        RefusalReason.SUBMIT_FAILED,
        message,
        site=site,
        exit_code=exit_code,
        retry=False,
        **data,
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


def site_offers(operations: Iterable[OperationRecord], site_id: str) -> dict[str, dict[str, Any]]:
    """Return the install settings failures on the site offered, by ``issue_id``.

    They are what relay already holds: a submit's refusal keeps its records'
    settings (:func:`failed`), and a failed install job's result keeps them
    (:func:`outcome`). The newest offer of an issue wins.
    """
    found: dict[str, dict[str, Any]] = {}
    for record in sorted(operations, key=lambda item: item.updated_at):
        if record.site != site_id:
            continue
        for data in ((record.error or {}).get("data"), record.result):
            if isinstance(data, dict):
                for setting in _dicts(cast(dict[str, Any], data).get(SETTINGS_KEY)):
                    if isinstance(setting.get("issue_id"), str):
                        found[str(setting["issue_id"])] = setting
    return found


def check_settings(spec: JobSpec, offers: Mapping[str, Mapping[str, Any]], *, site: str) -> None:
    """Refuse a job's install settings that no failure on the site offered.

    An install setting answers a question JARVIS put on a failure record; any
    other issue is a decision, and those reach JARVIS only as the person's
    answers to relay's asks.
    """
    unknown = [item.issue_id for item in spec.install_settings if item.issue_id not in offers]
    if not unknown:
        return
    raise Refusal(
        RefusalReason.INVALID_SPEC,
        f"no install failure on {site} offered the setting(s) {', '.join(unknown)}; "
        "install_settings take only the issue_ids of a failure record's settings",
        site=site,
        unknown=unknown,
        settings=list(offers.values()),
    )


def setting_answers(spec: JobSpec) -> dict[str, Any]:
    """Return the job's install settings as JARVIS's answers, by ``issue_id``."""
    answers: dict[str, Any] = {}
    for item in spec.install_settings:
        entry: dict[str, Any] = {"constraint": item.value, "answered_by": "agent"}
        if item.basis:
            entry["basis"] = item.basis
        answers[item.issue_id] = entry
    return answers

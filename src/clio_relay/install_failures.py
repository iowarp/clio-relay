"""JARVIS's typed install failures (exit 6), passed on as asks or errors.

When an installer fails, JARVIS classifies the output into one typed issue and
exits 6. Spack's are ``compiler_unavailable``, ``fetch_failed``,
``build_failed``, ``variant_conflict``, ``no_recipe``, ``concretize_failed`` and
``install_failed``; the other methods' end in ``_failed`` too
(``pip_install_failed``, ``conda_install_failed``, ``conda_bootstrap_failed``,
``container_pull_failed``, ``container_archive_failed``, ``venv_failed``), and a
container engine that cannot run here is ``container_engine_unusable``. Each
carries the ``host``, the
``stage``, the ``log_tail`` and a ``remedy`` (Spack's also the
``failed_node``). It is the last line of a ``ppl install`` or ``ppl submit``'s
output, and, for an install job, in ``<execution>/install/issues.jsonl``.

Relay does not read the installer's log itself. A failure the person answers (a
usable compiler, a spec constraint) becomes the usual install asks; any
other becomes an error with JARVIS's kind, message and remedy.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any, cast

from clio_relay import jarvis_report, jarvis_submit, site_facts
from clio_relay.jobs.refusals import Refusal, RefusalReason

EXIT_INSTALL_FAILED = 6
"""``ppl install`` or ``ppl submit`` exit: an installer failed, with a typed issue."""

KINDS = frozenset(
    {
        "compiler_unavailable",
        "fetch_failed",
        "build_failed",
        "variant_conflict",
        "no_recipe",
        "concretize_failed",
        "install_failed",
        # A container engine that is installed but cannot run here (podman's own error).
        "container_engine_unusable",
    }
)
CONCRETIZE_KINDS = frozenset({"variant_conflict", "no_recipe", "concretize_failed"})


def is_failure(kind: object) -> bool:
    """Say whether an issue kind is a typed install failure, of any install method."""
    return kind in KINDS or (isinstance(kind, str) and kind.endswith("_failed"))


KEPT = (
    "kind",
    "issue_id",
    "method",
    "host",
    "stage",
    "packages",
    "failed_node",
    "build_log",
    "log",
    "message",
    "remedy",
    "compiler",
    "missing_executables",
    "usable_compilers",
)
TAIL_LINES = 20


def _dicts(items: object) -> list[dict[str, Any]]:
    listed = cast(list[object], items) if isinstance(items, list) else []
    return [cast(dict[str, Any], item) for item in listed if isinstance(item, dict) and item]


def failures(issues: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return the failure issues, one per ``issue_id`` (the last one written wins)."""
    found: dict[str, dict[str, Any]] = {}
    for issue in issues:
        if is_failure(issue.get("kind")):
            found[str(issue.get("issue_id") or len(found))] = dict(issue)
    return list(found.values())


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


def _brief(issue: Mapping[str, Any]) -> dict[str, Any]:
    kept = {key: issue[key] for key in KEPT if issue.get(key) not in (None, "", [])}
    tail = issue.get("log_tail")
    lines = (
        tail.splitlines()
        if isinstance(tail, str)
        else [str(line) for line in cast(list[object], tail)]
        if isinstance(tail, list)
        else []
    )
    if lines:
        kept["log_tail"] = "\n".join(lines[-TAIL_LINES:])
    return kept


def install_failed(issues: list[dict[str, Any]], *, site: str, **extra: Any) -> Refusal:
    """The error for failures no answer fixes: JARVIS's kind, where, and its remedy."""
    first = issues[0]
    kind = str(first.get("kind") or "install_failed")
    where = ", ".join(f"{key} {first[key]}" for key in ("host", "stage") if first.get(key))
    message = f"JARVIS could not install the package's software on {site}: {kind}"
    if where:
        message = f"{message} ({where})"
    if first.get("failed_node"):
        message = f"{message} at {first['failed_node']}"
    if first.get("message"):
        message = f"{message}: {first['message']}"
    if first.get("remedy"):
        message = jarvis_report.then(message, str(first["remedy"]))
    reason = RefusalReason.SUBMIT_FAILED
    if kind == "fetch_failed":
        reason = RefusalReason.UNREACHABLE
    elif kind in CONCRETIZE_KINDS:
        reason = RefusalReason.INVALID_SPEC
    return Refusal(
        reason,
        message,
        kind=kind,
        kinds=sorted({str(issue.get("kind")) for issue in issues}),
        issues=[_brief(issue) for issue in issues],
        exit_code=EXIT_INSTALL_FAILED,
        retry=False,
        **extra,
    )


def asks_again(issues: list[dict[str, Any]], *, site: str) -> Refusal:
    """The install refusal for failures the person answers; relay asks, then installs again."""
    failed = install_failed(issues, site=site)
    report: dict[str, Any] = {"issues": issues, "installers": {}}
    refusal = jarvis_report.install_needed(report, site=site)
    refusal.data["ask_message"] = (
        f"{failed.message}. JARVIS asks {len(issues)} question(s) about it; with your "
        "answers it runs the install job again. Go ahead?"
    )
    refusal.data["failure"] = failed.data
    return refusal


def from_issues(issues: list[dict[str, Any]], *, site: str, **extra: Any) -> Refusal | None:
    """Return the asks or the error for an install that failed, or None with no failures."""
    failed = failures(issues)
    if not failed:
        return None
    report = {"issues": failed}
    if jarvis_report.choice_issues(report) and not jarvis_report.blocking_issues(report):
        return asks_again(failed, site=site)
    return install_failed(failed, site=site, **extra)


def refusal(text: str, *, site: str) -> Refusal:
    """The refusal for a ``ppl install`` or ``ppl submit`` that exited 6."""
    report = jarvis_report.parse_install_report(text) or {}
    named = {str(item) for item in cast(list[object], report.get("failures") or [])}
    issues = [
        issue
        for issue in _dicts(report.get("issues"))
        if not named or str(issue.get("issue_id")) in named
    ]
    found = from_issues(issues, site=site)
    if found is not None:
        return found
    tail = "\n".join(text.strip().splitlines()[-5:]) or "no output"
    return Refusal(
        RefusalReason.SUBMIT_FAILED,
        f"JARVIS on {site} exited {EXIT_INSTALL_FAILED} (the install failed) with no typed "
        f"issue: {tail}",
        exit_code=EXIT_INSTALL_FAILED,
    )


def raise_for(exit_code: object, text: str, *, site: str) -> None:
    """Raise JARVIS's typed exits relay passes on before its own handling.

    Exit 6 is a typed install failure (:func:`refusal`); exit 3 with a blocking
    site-fact question is :func:`clio_relay.site_facts.refusal`. Any other exit 3
    is an install report, which the caller turns into the install asks.
    """
    if str(exit_code) == str(jarvis_submit.EXIT_DECISION):
        site_facts.raise_for(text, site=site)
    if str(exit_code) == str(EXIT_INSTALL_FAILED):
        raise refusal(text, site=site)

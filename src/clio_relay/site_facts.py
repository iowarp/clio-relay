"""JARVIS's site-fact questions (``site_fact_uncertain``), passed on untouched.

When a deployment pick rests on a site fact JARVIS cannot settle by itself
(the MPI launch network, first), JARVIS raises a ``site_fact_uncertain``
issue with its evidence, candidates and read-only probes. A blocking one stops
``ppl check`` (``questions[]``) and ``ppl submit`` with exit 3 before anything
reaches the scheduler; an advisory one rides along in the check's
``warnings[]`` and the execution's ``metadata.site_facts``.

Relay adds no network logic. It refuses the run with JARVIS's issues as they
are, and the agent answers through relay's JARVIS exec (``rg facts show
+json``, then, once the person confirms, ``rg facts answer <issue_id> --value
... --reason ... --set_by <person> --proposed_by <agent> +json``) before it
submits again.
"""

from __future__ import annotations

import json
import re
from typing import Any, cast

from clio_relay.jobs.refusals import Refusal, RefusalReason

KIND = "site_fact_uncertain"
ANSWER = "jarvis rg facts answer"


def _documents(text: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    found: list[dict[str, Any]] = []
    for match in re.finditer(r"(?m)^\s*\{", text):
        try:
            document, _end = decoder.raw_decode(text[match.start() :].lstrip())
        except ValueError:
            continue
        if isinstance(document, dict):
            found.append(cast(dict[str, Any], document))
    return found


def _blocking(issue: dict[str, Any]) -> bool:
    details = issue.get("details")
    severity = cast(dict[str, Any], details).get("severity") if isinstance(details, dict) else None
    return issue.get("blocking") is True or "block" in (issue.get("severity"), severity)


def questions(text: str) -> list[dict[str, Any]]:
    """Return the blocking ``site_fact_uncertain`` issues in JARVIS's output, once each."""
    found: dict[str, dict[str, Any]] = {}
    for document in _documents(text):
        for key in ("questions", "issues"):
            listed = document.get(key)
            for item in cast(list[object], listed) if isinstance(listed, list) else []:
                if isinstance(item, dict):
                    issue = cast(dict[str, Any], item)
                    if issue.get("kind") == KIND and _blocking(issue):
                        found[str(issue.get("issue_id") or len(found))] = issue
    return list(found.values())


def refusal(issues: list[dict[str, Any]], *, site: str) -> Refusal:
    """The refusal for a run JARVIS holds until the person confirms a site fact."""
    first = issues[0]
    fact = first.get("field") or first.get("subject") or "a site fact"
    said = "; ".join(str(issue["message"]) for issue in issues if issue.get("message"))
    message = (
        f"JARVIS on {site} needs a person to confirm {fact} before it submits"
        f"{': ' + said if said else ''}. Investigate with the issue's probes, propose a "
        f"value, and once the person confirms, answer through relay's JARVIS exec "
        f"({ANSWER} <issue_id> ...); then run again"
    )
    return Refusal(
        RefusalReason.INPUT_MISSING,
        message,
        kind=KIND,
        issues=issues,
        needs=ANSWER,
        retry=True,
    )


def raise_for(text: str, *, site: str) -> None:
    """Raise :func:`refusal` when JARVIS's output holds a blocking site-fact question."""
    found = questions(text)
    if found:
        raise refusal(found, site=site)

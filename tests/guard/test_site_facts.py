from __future__ import annotations

import json
from typing import Any

import pytest

from clio_relay import install_failures, site_facts
from clio_relay.jarvis_submit import record_status
from clio_relay.jobs.refusals import Refusal, RefusalReason


def _issue(severity: str, issue_id: str = "sha256:aa") -> dict[str, Any]:
    return {
        "schema_version": "jarvis.issue.v1",
        "kind": "site_fact_uncertain",
        "subject": "net",
        "field": "resource_graph.site_facts.net.launch",
        "issue_id": issue_id,
        "severity": severity,
        "blocking": severity == "block",
        "message": "the job asks 4 nodes; the graph saw 2",
        "details": {"severity": severity, "fact": "net.launch", "probes": [{"command": "ip -j a"}]},
        "candidates": [{"rank": 1, "cidr": "10.0.0.0/24"}],
    }


def test_a_blocking_check_question_refuses_the_run_with_jarvis_issue_untouched() -> None:
    blocking = _issue("block")
    check = {
        "schema_version": "jarvis.pipeline-check.v1",
        "ok": False,
        "questions": [blocking],
        "warnings": [_issue("advisory", "sha256:bb")],
    }
    text = json.dumps(check, indent=2)
    with pytest.raises(Refusal) as caught:
        install_failures.raise_for(3, text, site="ares")
    refusal = caught.value
    assert refusal.reason == RefusalReason.INPUT_MISSING
    assert refusal.data["kind"] == "site_fact_uncertain"
    assert refusal.data["issues"] == [blocking]
    assert refusal.data["needs"] == "jarvis rg facts answer"
    assert "resource_graph.site_facts.net.launch" in refusal.message


def test_a_submit_exit_3_with_blocking_issues_is_a_site_fact_question() -> None:
    text = "noise\n" + json.dumps({"exit_code": 3, "issues": [_issue("block")]})
    assert [issue["issue_id"] for issue in site_facts.questions(text)] == ["sha256:aa"]


def test_advisory_questions_and_install_reports_do_not_block() -> None:
    advisory = json.dumps({"ok": True, "questions": [], "warnings": [_issue("advisory")]})
    install_failures.raise_for(3, advisory, site="ares")
    install_failures.raise_for(0, json.dumps({"issues": [_issue("block")]}), site="ares")


def test_the_execution_carries_jarvis_site_facts_untouched() -> None:
    facts = {
        "schema_version": "jarvis.site-facts.v1",
        "issues": [_issue("advisory")],
        "confirmed": [],
        "stages": [{"stage": "check", "blocking": False}],
    }
    status = record_status({"state": "running", "metadata": {"site_facts": facts}})
    assert status.site_facts == facts
    assert record_status({"state": "running"}).site_facts is None

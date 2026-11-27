from __future__ import annotations

import json
from typing import Any

import pytest

from clio_relay import install_failures, site_facts
from clio_relay.jarvis_submit import record_status
from clio_relay.jobs.handles import JobState
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


def _ucx_unusable() -> dict[str, Any]:
    issue = _issue("advisory", "sha256:cc")
    issue["details"]["reasons"] = [
        {
            "code": "ucx_device_unusable",
            "severity": "advisory",
            "roles": ["data"],
            "message": "UCX on comp-05 cannot use mlx5_0:1 (not_listed_by_ucx)",
            "ucx": {
                "comp-05": {
                    "status": "not_listed_by_ucx",
                    "port": "mlx5_0:1",
                    "link_layer": "InfiniBand",
                    "ucx_rdma_devices": [],
                }
            },
        }
    ]
    return issue


def test_a_ucx_device_unusable_advisory_passes_through_untouched() -> None:
    advisory = _ucx_unusable()
    check = json.dumps({"ok": True, "questions": [], "warnings": [advisory]})
    install_failures.raise_for(3, check, site="ares")
    assert site_facts.questions(check) == []
    facts = {"schema_version": "jarvis.site-facts.v1", "issues": [advisory], "confirmed": []}
    status = record_status({"state": "succeeded", "metadata": {"site_facts": facts}})
    assert status.site_facts == facts


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


def _net_not_collected() -> dict[str, Any]:
    issue = _issue("advisory", "sha256:dd")
    issue["answer"] = {"values": []}
    issue["details"].update({"reason": "net_not_collected", "plan": "collect_at_launch"})
    return issue


def test_a_net_not_collected_advisory_and_the_net_plan_pass_through_untouched() -> None:
    advisory = _net_not_collected()
    plan = {
        "action": "collect_at_launch",
        "reasons": ["net_missing"],
        "note": "the launch collects the net facts on every launch host first",
    }
    check = {
        "schema_version": "jarvis.pipeline-check.v1",
        "ok": True,
        "questions": [],
        "warnings": [advisory],
        "site_facts": {"net_present": False, "net_plan": plan, "issues": [advisory]},
    }
    text = json.dumps(check)
    assert site_facts.questions(text) == []
    install_failures.raise_for(3, text, site="ares")
    facts = {
        "schema_version": "jarvis.site-facts.v1",
        "net_present": False,
        "net_plan": plan,
        "issues": [advisory],
        "confirmed": [],
    }
    status = record_status({"state": "running", "metadata": {"site_facts": facts}})
    assert status.site_facts == facts


@pytest.mark.parametrize(
    "kind",
    [
        "resource_graph_collect_failed",
        "resource_graph_collect_partial",
        "resource_graph_net_invalid",
    ],
)
def test_a_launch_refusal_in_a_submitted_job_is_a_typed_failure(kind: str) -> None:
    issue = {
        "schema_version": "jarvis.issue.v1",
        "kind": kind,
        "field": "resource_graph.net",
        "message": f"{kind}: comp-05 returned no net facts",
        "details": {
            "stage": "launch",
            "collected_hosts": ["comp-03"],
            "failed_hosts": [{"host": "comp-05", "via": "ssh", "exit_code": 255}],
        },
    }
    record = {"state": "failed", "return_code": 5, "metadata": {"issues": [issue]}}
    status = record_status(record)
    assert status.state == JobState.FAILED
    assert status.exit_code == 5
    assert status.issues == [issue]
    assert status.message is not None and f"{kind}: comp-05" in status.message


def test_a_launch_collected_block_fails_the_job_and_the_next_check_asks() -> None:
    blocking = _issue("block", "sha256:ee")
    blocking["details"].update(
        {"stage": "launch_collected", "collected_at_launch": {"hosts": ["comp-03", "comp-04"]}}
    )
    record = {"state": "failed", "return_code": 3, "metadata": {"issues": [blocking]}}
    status = record_status(record)
    assert status.state == JobState.FAILED
    assert status.exit_code == 3
    assert status.issues == [blocking]

    check = {"schema_version": "jarvis.pipeline-check.v1", "ok": False, "questions": [blocking]}
    with pytest.raises(Refusal) as caught:
        install_failures.raise_for(3, json.dumps(check), site="ares")
    assert caught.value.reason == RefusalReason.INPUT_MISSING
    assert caught.value.data["issues"] == [blocking]

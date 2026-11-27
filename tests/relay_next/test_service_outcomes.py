from __future__ import annotations

from typing import Any

from clio_relay.jarvis_submit import record_status
from clio_relay.jobs.handles import JobState
from clio_relay.surface.results import crashed_services, operation_result


def _outcomes(*, crashed: bool = True) -> dict[str, Any]:
    service: dict[str, Any] = {
        "pkg_id": "viz",
        "package": "clio_jarvis.pvserver",
        "outcome": "crashed" if crashed else "stopped",
        "after_steps": "stop",
        "after_ready": True,
        "during_steps": True,
    }
    if crashed:
        service["error"] = "pvserver exited with signal 11"
    return {
        "schema_version": "jarvis.execution.outcomes.v1",
        "application": {"state": "completed", "steps": ["md"], "failed_package": None},
        "services": [service],
        "services_crashed": ["viz"] if crashed else [],
    }


def test_the_execution_carries_jarvis_outcomes_untouched() -> None:
    outcomes = _outcomes()
    status = record_status(
        {"state": "completed", "return_code": 0, "metadata": {"outcomes": outcomes}}
    )
    assert status.state == JobState.COMPLETED
    assert status.exit_code == 0
    assert status.outcomes == outcomes
    assert record_status({"state": "running"}).outcomes is None


def test_a_crashed_service_is_said_apart_from_the_application() -> None:
    said = crashed_services(_outcomes())
    assert "The application finished, but its service viz crashed while the steps ran" in said
    assert "signal 11" in said
    assert crashed_services(_outcomes(crashed=False)) == ""
    assert crashed_services({}) == ""


def test_a_completed_job_with_a_crashed_service_is_not_an_error_result() -> None:
    view = {
        "operation_id": "op_1",
        "site": "ares",
        "state": "completed",
        "exit_code": 0,
        "result": {"outcomes": _outcomes()},
    }
    result = operation_result(view)
    assert result.is_error is False
    text = result.content[0].text  # type: ignore[union-attr]
    assert "is completed with exit code 0" in text
    assert "its service viz crashed" in text

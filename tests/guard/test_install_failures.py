from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from clio_relay import install_failures
from clio_relay.jarvis_submit import INSTALL_REPORT_SCHEMA, NEEDS_INSTALL
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.surface import installing

BUILD_FAILED: dict[str, Any] = {
    "issue_id": "f1",
    "kind": "build_failed",
    "host": "ares-comp-05",
    "stage": "install",
    "packages": ["paraview"],
    "failed_node": "paraview@5.13.1",
    "log_tail": "\n".join(f"line {n}" for n in range(40)),
    "message": "paraview failed to build",
    "remedy": "Read the build log, then constrain the spec.",
}
COMPILER: dict[str, Any] = {
    "issue_id": "c1",
    "kind": "compiler_unavailable",
    "host": "ares-comp-05",
    "stage": "install",
    "answerable": True,
    "question": "Which compiler should JARVIS build with?",
    "candidates": [{"id": "gcc@11.4.0", "constraint": "gcc@11.4.0"}],
    "message": "gcc@12.3.0 is not usable on the build host",
    "remedy": "Pick a usable compiler.",
}


def _report(*issues: dict[str, Any], failures: list[str] | None = None) -> str:
    document: dict[str, Any] = {
        "schema_version": INSTALL_REPORT_SCHEMA,
        "failed": True,
        "failures": failures if failures is not None else [i["issue_id"] for i in issues],
        "installers": {},
        "issues": list(issues),
    }
    return f"spack said things\n{json.dumps(document)}\n"


def test_a_build_failure_is_a_typed_error_without_the_raw_log() -> None:
    refusal = install_failures.refusal(_report(BUILD_FAILED), site="ares")
    assert refusal.reason == RefusalReason.SUBMIT_FAILED
    assert refusal.data["kind"] == "build_failed"
    assert refusal.data["exit_code"] == 6
    assert "ares-comp-05" in refusal.message
    assert "paraview@5.13.1" in refusal.message
    assert "constrain the spec" in refusal.message
    assert "line 39" not in refusal.message
    tail = refusal.data["issues"][0]["log_tail"].splitlines()
    assert tail[0] == "line 20" and tail[-1] == "line 39"


@pytest.mark.parametrize(
    ("kind", "reason"),
    [
        ("fetch_failed", RefusalReason.UNREACHABLE),
        ("concretize_failed", RefusalReason.INVALID_SPEC),
        ("variant_conflict", RefusalReason.INVALID_SPEC),
        ("install_failed", RefusalReason.SUBMIT_FAILED),
    ],
)
def test_each_kind_keeps_its_name_and_a_reason(kind: str, reason: RefusalReason) -> None:
    refusal = install_failures.refusal(_report({**BUILD_FAILED, "kind": kind}), site="ares")
    assert refusal.reason == reason
    assert refusal.data["kind"] == kind


def test_a_compiler_the_person_picks_becomes_the_install_asks() -> None:
    refusal = install_failures.refusal(_report(COMPILER), site="ares")
    assert refusal.data["needs"] == NEEDS_INSTALL
    assert installing.install_kind(refusal) == "software"
    assert refusal.data["install_report"]["issues"][0]["issue_id"] == "c1"
    assert "compiler_unavailable" in refusal.data["ask_message"]
    assert refusal.data["failure"]["kind"] == "compiler_unavailable"


def test_only_the_named_failures_count() -> None:
    other = {**BUILD_FAILED, "issue_id": "f2", "kind": "fetch_failed"}
    refusal = install_failures.refusal(_report(BUILD_FAILED, other, failures=["f2"]), site="ares")
    assert refusal.data["kind"] == "fetch_failed"


def test_exit_6_with_no_typed_issue_says_so() -> None:
    refusal = install_failures.refusal("Error: something\n", site="ares")
    assert refusal.reason == RefusalReason.SUBMIT_FAILED
    assert "no typed issue" in refusal.message


def test_raise_for_raises_only_on_6() -> None:
    install_failures.raise_for(5, _report(BUILD_FAILED), site="ares")
    install_failures.raise_for(None, _report(BUILD_FAILED), site="ares")
    with pytest.raises(Refusal):
        install_failures.raise_for("6", _report(BUILD_FAILED), site="ares")


def test_issues_jsonl_keeps_the_last_line_per_issue() -> None:
    text = "\n".join(
        [json.dumps({**BUILD_FAILED, "message": "first"}), "not json", json.dumps(BUILD_FAILED)]
    )
    found = install_failures.failures(install_failures.read_jsonl(text))
    assert len(found) == 1
    assert found[0]["message"] == "paraview failed to build"


class _Files:
    def __init__(self, text: str) -> None:
        self.text = text
        self.paths: list[str] = []

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        self.paths.append(path)
        return self.text.encode()


class _Adapter:
    implements = frozenset({"files"})

    def __init__(self, text: str) -> None:
        self._files = _Files(text)

    async def files(self) -> _Files:
        return self._files


class _Site:
    title = "Ares"


class _Record:
    site = "ares"


class _Service:
    def __init__(self) -> None:
        self.finished: list[tuple[OperationState, Refusal | None]] = []

    def operation(self, operation_id: str) -> _Record:
        return _Record()

    def site(self, site_id: str) -> _Site:
        return _Site()

    def finish(self, operation_id: str, state: OperationState, refusal: Refusal | None) -> str:
        self.finished.append((state, refusal))
        return "record"


@pytest.mark.asyncio
async def test_an_install_job_that_exits_6_fails_with_its_typed_issue() -> None:
    adapter = _Adapter(json.dumps(BUILD_FAILED) + "\n")
    service = _Service()
    handle = JobHandle(
        adapter="ssh",
        native_id="27990",
        data={"execution_dir": "/home/u/jobs/op/executions/e1"},
    )
    status = JobStatus(
        state=JobState.FAILED, exit_code=6, observed_at=datetime.now(UTC), native_state="FAILED"
    )
    result = await installing._ended(  # pyright: ignore[reportPrivateUsage]
        service,  # pyright: ignore[reportArgumentType]
        "op",
        status,
        (adapter, handle),  # pyright: ignore[reportArgumentType]
        "raw tail",
    )
    assert result == "record"
    assert adapter._files.paths == ["/home/u/jobs/op/executions/e1/install/issues.jsonl"]  # pyright: ignore[reportPrivateUsage]
    state, refusal = service.finished[0]
    assert state == OperationState.FAILED
    assert refusal is not None
    assert refusal.data["kind"] == "build_failed"
    assert refusal.data["install_job"] == "27990"
    assert "raw tail" not in refusal.message

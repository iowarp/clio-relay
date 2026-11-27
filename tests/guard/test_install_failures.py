from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from clio_relay import install_failures
from clio_relay.jarvis_submit import INSTALL_REPORT_SCHEMA
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.surface import installing

OUTPUT = "/home/u/.clio-relay/install/failures/install-ares-comp-05-0123456789ab.log"
BUILD: dict[str, Any] = {
    "schema": "jarvis.install-issue.v1",
    "failure_schema": "jarvis.failure.v1",
    "issue_id": "sha256:f1",
    "kind": "failure",
    "package": "paraview",
    "subject": "install",
    "scope": "run",
    "answerable": False,
    "candidates": [],
    "evidence": [],
    "stage": "install",
    "doing": "building paraview@5.13.1 with Spack",
    "message": (f"spack install paraview failed on ares-comp-05 (exit 1); full output: {OUTPUT}"),
    "question": (f"spack install paraview failed on ares-comp-05 (exit 1); full output: {OUTPUT}"),
    "host": "ares-comp-05",
    "command": "spack install paraview@5.13.1",
    "cwd": "/home/u/jobs/op/executions/e1",
    "exit_code": 1,
    "packages": ["paraview"],
    "failed_node": "paraview@5.13.1",
    "output_tail": "\n".join(f"line {n}" for n in range(40)),
    "output_path": OUTPUT,
    "output_bytes": 81234,
    "paths": {"install_log": "/x/install.log", "issues": "/x/issues.jsonl", "build_log": "/b"},
}
PROBE: dict[str, Any] = {
    **BUILD,
    "issue_id": "sha256:p1",
    "stage": "compiler_probe",
    "package": "*",
    "message": "gcc@12.3.0 cannot create executables on ares-comp-05; full output: /p.log",
    "question": "gcc@12.3.0 cannot create executables on ares-comp-05; full output: /p.log",
    "compiler": "gcc@12.3.0",
    "probe_step": "compile",
}


def _report(*issues: dict[str, Any], failures: list[str] | None = None) -> str:
    document: dict[str, Any] = {
        "schema_version": INSTALL_REPORT_SCHEMA,
        "failed": True,
        "exit_code": 6,
        "satisfied": False,
        "failures": failures if failures is not None else [i["issue_id"] for i in issues],
        "installers": {},
        "issues": list(issues),
        "open_issues": [i["issue_id"] for i in issues],
        "issues_path": "/x/issues.jsonl",
        "record_path": "/x/install-check.json",
    }
    return f"spack said things\n{json.dumps(document)}\n"


def test_a_failure_is_passed_whole_in_jarvis_s_words() -> None:
    refusal = install_failures.refusal(_report(BUILD), site="Ares")
    assert refusal.reason == RefusalReason.SUBMIT_FAILED
    assert refusal.message == BUILD["message"]
    assert refusal.data["failures"] == [BUILD]
    assert refusal.data["exit_code"] == 6
    assert refusal.data["retry"] is False
    assert refusal.data["issues_path"] == "/x/issues.jsonl"
    assert refusal.data["record_path"] == "/x/install-check.json"
    assert refusal.data["report"]["failed"] is True
    assert installing.install_kind(refusal) is None


def test_a_failure_is_never_an_ask_even_with_candidates_beside_it() -> None:
    question = {
        "issue_id": "q1",
        "kind": "ambiguous_provider",
        "answerable": True,
        "question": "Which MPI?",
        "candidates": [{"id": "openmpi", "constraint": "^openmpi"}],
    }
    refusal = install_failures.refusal(_report(PROBE, question, failures=["sha256:p1"]), site="A")
    assert installing.install_kind(refusal) is None
    assert refusal.message == PROBE["message"]
    assert refusal.data["failures"] == [PROBE]
    assert refusal.data["issues"] == [PROBE, question]


def test_every_failure_is_listed_one_line_each() -> None:
    refusal = install_failures.refusal(_report(BUILD, PROBE), site="Ares")
    assert refusal.message.splitlines() == [BUILD["message"], PROBE["message"]]
    assert [item["issue_id"] for item in refusal.data["failures"]] == ["sha256:f1", "sha256:p1"]


def test_exit_6_with_no_record_passes_what_jarvis_printed() -> None:
    refusal = install_failures.refusal("Error: something\n", site="Ares")
    assert refusal.reason == RefusalReason.SUBMIT_FAILED
    assert refusal.message == "Error: something"
    assert refusal.data["failures"] == []


def test_raise_for_raises_only_on_6() -> None:
    install_failures.raise_for(5, _report(BUILD), site="Ares")
    install_failures.raise_for(None, _report(BUILD), site="Ares")
    with pytest.raises(Refusal):
        install_failures.raise_for("6", _report(BUILD), site="Ares")


def test_issues_jsonl_keeps_the_last_line_per_issue() -> None:
    text = "\n".join([json.dumps({**BUILD, "message": "first"}), "not json", json.dumps(BUILD)])
    found = install_failures.records(install_failures.read_jsonl(text))
    assert found == [BUILD]


def test_a_job_s_other_unanswerable_issues_are_not_failures() -> None:
    missing = {"issue_id": "n1", "kind": "not_installed", "answerable": False}
    unchecked = {"issue_id": "u1", "kind": "install_unchecked", "answerable": False}
    no_compiler = {"issue_id": "c1", "kind": "compiler_unavailable", "answerable": False}
    pickable = {**no_compiler, "issue_id": "c2", "answerable": True}
    found = install_failures.records([missing, unchecked, BUILD, no_compiler, pickable])
    assert found == [BUILD, no_compiler]


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


def _status(code: int) -> JobStatus:
    return JobStatus(
        state=JobState.FAILED, exit_code=code, observed_at=datetime.now(UTC), native_state="FAILED"
    )


def _handle() -> JobHandle:
    return JobHandle(
        adapter="ssh", native_id="27990", data={"execution_dir": "/home/u/jobs/op/executions/e1"}
    )


async def _end(
    monkeypatch: pytest.MonkeyPatch, text: str, code: int, tail: str | None
) -> tuple[_Adapter, _Service, object]:
    async def resubmit(*args: object) -> str:
        raise AssertionError("relay must not check again on the login host")

    monkeypatch.setattr(installing, "_resubmit", resubmit)
    adapter, service = _Adapter(text), _Service()
    result = await installing._ended(  # pyright: ignore[reportPrivateUsage]
        service,  # pyright: ignore[reportArgumentType]
        "op",
        _status(code),
        (adapter, _handle()),  # pyright: ignore[reportArgumentType]
        tail,
    )
    return adapter, service, result


@pytest.mark.asyncio
async def test_an_install_job_that_exits_6_fails_with_its_records_whole(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, service, result = await _end(monkeypatch, json.dumps(BUILD) + "\n", 6, "raw tail")
    assert result == "record"
    assert adapter._files.paths == ["/home/u/jobs/op/executions/e1/install/issues.jsonl"]  # pyright: ignore[reportPrivateUsage]
    state, refusal = service.finished[0]
    assert state == OperationState.FAILED
    assert refusal is not None
    assert refusal.message == BUILD["message"]
    assert refusal.data["failures"] == [BUILD]
    assert refusal.data["install_job"] == "27990"
    assert installing.install_kind(refusal) is None


@pytest.mark.asyncio
async def test_an_install_job_that_exits_6_with_no_record_passes_its_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _adapter, service, _result = await _end(monkeypatch, "", 6, "==> Error: out of space\n")
    _state, refusal = service.finished[0]
    assert refusal is not None
    assert refusal.message == "==> Error: out of space"


@pytest.mark.asyncio
async def test_an_install_job_that_ends_otherwise_passes_its_output_as_printed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _adapter, service, _result = await _end(monkeypatch, "", 1, "slurmstepd: CANCELLED\n")
    _state, refusal = service.finished[0]
    assert refusal is not None
    assert refusal.message == "slurmstepd: CANCELLED"
    assert refusal.data["exit_code"] == 1
    assert "JARVIS could not" not in refusal.message


@pytest.mark.asyncio
async def test_an_install_job_with_an_issue_no_answer_settles_fails_in_jarvis_s_words(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stuck: dict[str, Any] = {
        "issue_id": "q1",
        "kind": "mpi_unavailable",
        "answerable": True,
        "candidates": [],
        "detail": "no MPI on the compute nodes",
        "hint": "Load an MPI module first.",
    }
    _adapter, service, _result = await _end(monkeypatch, json.dumps(stuck) + "\n", 3, None)
    state, refusal = service.finished[0]
    assert state == OperationState.FAILED
    assert refusal is not None
    assert refusal.message == "no MPI on the compute nodes. Load an MPI module first."
    assert refusal.data["issues"] == [stuck]


@pytest.mark.asyncio
async def test_an_install_job_with_a_question_checks_again_on_the_login_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notes: list[str] = []

    async def resubmit(service: object, operation_id: str, note: str) -> str:
        notes.append(note)
        return "resubmitted"

    monkeypatch.setattr(installing, "_resubmit", resubmit)

    def update(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(installing, "_update", update)
    question = {
        "issue_id": "q2",
        "kind": "ambiguous_provider",
        "answerable": True,
        "question": "Which MPI should JARVIS use?",
        "candidates": [{"id": "openmpi", "constraint": "^openmpi"}],
    }
    adapter, service = _Adapter(json.dumps(question) + "\n"), _Service()
    result = await installing._ended(  # pyright: ignore[reportPrivateUsage]
        service,  # pyright: ignore[reportArgumentType]
        "op",
        _status(3),
        (adapter, _handle()),  # pyright: ignore[reportArgumentType]
        None,
    )
    assert result == "resubmitted"
    assert service.finished == []
    assert notes == ["the install job needs a decision; JARVIS checks again"]


TMPDIR_ID = "sha256:" + "7" * 64
TMPDIR_SETTING: dict[str, Any] = {
    "setting": "install_tmpdir",
    "issue_id": TMPDIR_ID,
    "scope": "site",
    "question": "Which directory should this site's install jobs use for temporary files?",
    "answer": "a directory with room for the build's temporary files; recorded for this site",
    "value": None,
    "tmpdir": None,
    "build_stage": None,
    "source": None,
    "unset": "the job's own TMPDIR and Spack's own config:build_stage apply",
    "recorded_in": "/home/u/.clio-relay/jarvis/decisions.yaml",
    "forget": "jarvis ppl install --forget install_tmpdir",
}


def test_a_failure_s_install_settings_are_offered_whole_in_jarvis_s_words() -> None:
    build = {**BUILD, "settings": [TMPDIR_SETTING]}
    probe = {**PROBE, "settings": [TMPDIR_SETTING]}
    refusal = install_failures.refusal(_report(build, probe), site="Ares")
    assert refusal.data["settings"] == [TMPDIR_SETTING]
    assert refusal.data["failures"] == [build, probe]
    lines = refusal.message.splitlines()
    assert lines[:2] == [BUILD["message"], PROBE["message"]]
    assert lines[2] == f"{TMPDIR_SETTING['question']} ({TMPDIR_SETTING['answer']})"
    assert "install_settings" in refusal.data["settings_how"]


def test_a_failure_without_settings_offers_none() -> None:
    refusal = install_failures.refusal(_report(BUILD), site="Ares")
    assert "settings" not in refusal.data
    assert "settings_how" not in refusal.data


def _failed_op(site: str, settings: list[dict[str, Any]]) -> Any:
    from clio_relay.jobs.records import OperationRecord

    refusal = install_failures.failed([{**BUILD, "settings": settings}], site=site)
    return OperationRecord(
        operation_id="op-f",
        kind="install",
        site=site,
        adapter="ssh",
        spec={},
        spec_digest="d",
        state=OperationState.FAILED,
        error=refusal.to_dict(),
    )


def _job(*settings: dict[str, Any]) -> Any:
    from clio_relay.jobs.spec import JobSpec

    return JobSpec.model_validate(
        {"package": {"name": "clio_jarvis.gray_scott"}, "install_settings": list(settings)}
    )


def test_install_settings_take_only_what_a_failure_on_the_site_offered() -> None:
    offers = install_failures.site_offers([_failed_op("ares", [TMPDIR_SETTING])], "ares")
    assert offers == {TMPDIR_ID: TMPDIR_SETTING}
    job = _job({"issue_id": TMPDIR_ID, "value": "/mnt/scratch/u/tmp", "basis": "the person"})
    install_failures.check_settings(job, offers, site="ares")
    assert install_failures.setting_answers(job) == {
        TMPDIR_ID: {
            "constraint": "/mnt/scratch/u/tmp",
            "answered_by": "agent",
            "basis": "the person",
        }
    }
    elsewhere = install_failures.site_offers([_failed_op("other", [TMPDIR_SETTING])], "ares")
    with pytest.raises(Refusal) as refused:
        install_failures.check_settings(job, elsewhere, site="ares")
    assert refused.value.reason == RefusalReason.INVALID_SPEC
    assert refused.value.data["unknown"] == [TMPDIR_ID]


def test_install_settings_cannot_answer_a_decision() -> None:
    offers = install_failures.site_offers([_failed_op("ares", [TMPDIR_SETTING])], "ares")
    placement = "sha256:" + "8" * 64
    with pytest.raises(Refusal):
        install_failures.check_settings(
            _job({"issue_id": placement, "value": "login_limited"}), offers, site="ares"
        )


def test_install_settings_stay_out_of_older_digests() -> None:
    from clio_relay.jobs.spec import JobSpec

    plain = JobSpec.model_validate({"package": {"name": "clio_jarvis.gray_scott"}})
    assert "install_settings" not in plain.dump()
    given = _job({"issue_id": TMPDIR_ID, "value": "/tmp/x"})
    assert given.digest() != plain.digest()
    with pytest.raises(ValueError):
        JobSpec.model_validate(
            {
                "executable": "/bin/true",
                "install_settings": [{"issue_id": TMPDIR_ID, "value": "/x"}],
            }
        )

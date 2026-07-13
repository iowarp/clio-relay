from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay import endpoint as endpoint_module
from clio_relay.jarvis_execution import (
    run_native_jarvis_broker,
    scheduled_jarvis_command,
)

_CREATED_AT = "2026-07-12T00:00:00Z"
_UPDATED_AT = "2026-07-12T00:00:01Z"
_MARKER = "clio-relay-0123456789abcdef"


@dataclass(frozen=True)
class _Handle:
    execution_id: str
    pipeline_id: str
    mode: str
    scheduler_provider: str | None = None
    scheduler_native_id: str | None = None
    cluster: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "jarvis.execution.handle.v1",
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "mode": self.mode,
            "scheduler_provider": self.scheduler_provider,
            "scheduler_native_id": self.scheduler_native_id,
            "cluster": self.cluster,
        }


@dataclass(frozen=True)
class _Record:
    execution_id: str
    pipeline_id: str
    mode: str
    state: str
    submitted: bool
    terminal: bool
    scheduler_provider: str | None = None
    scheduler_native_id: str | None = None
    cluster: str | None = None
    return_code: int | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=lambda: {})
    updated_at: str = _UPDATED_AT

    @property
    def handle(self) -> _Handle:
        return _Handle(
            execution_id=self.execution_id,
            pipeline_id=self.pipeline_id,
            mode=self.mode,
            scheduler_provider=self.scheduler_provider,
            scheduler_native_id=self.scheduler_native_id,
            cluster=self.cluster,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "jarvis.execution.record.v1",
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_id,
            "mode": self.mode,
            "scheduler_provider": self.scheduler_provider,
            "scheduler_native_id": self.scheduler_native_id,
            "cluster": self.cluster,
            "state": self.state,
            "submitted": self.submitted,
            "terminal": self.terminal,
            "created_at": _CREATED_AT,
            "updated_at": self.updated_at,
            "return_code": self.return_code,
            "error": self.error,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class _Progress:
    record: _Record

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "jarvis.execution.progress.v1",
            "execution_id": self.record.execution_id,
            "pipeline_id": self.record.pipeline_id,
            "execution_state": self.record.state,
            "terminal": self.record.terminal,
            "packages": [],
        }


def _intent(*, scheduler_expected: bool | str) -> dict[str, object]:
    return {
        "execution_id": "jarvis_test_execution",
        "marker": _MARKER,
        "scheduler_expected": scheduler_expected,
    }


def _yielding_sleep(_seconds: float) -> None:
    time.sleep(0.001)


class _DirectPipeline:
    scheduler = None

    def __init__(self, *, terminal: bool = True) -> None:
        self.name = "direct-pipeline"
        self.record: _Record | None = None
        self.finish = threading.Event()
        self.terminal = terminal
        self.run_calls: list[tuple[str, bool]] = []

    def run(self, *, execution_id: str, wait: bool) -> _Handle:
        self.run_calls.append((execution_id, wait))
        self.record = _Record(
            execution_id=execution_id,
            pipeline_id=self.name,
            mode="direct",
            state="running",
            submitted=False,
            terminal=False,
        )
        if self.terminal:
            if not self.finish.wait(timeout=5):
                raise RuntimeError("test did not release direct execution")
            self.record = replace(
                self.record,
                state="completed",
                terminal=True,
                return_code=0,
                updated_at="2026-07-12T00:00:02Z",
            )
        return self.record.handle

    def get_execution(self, execution_id: str) -> _Record:
        if self.record is None:
            raise FileNotFoundError(execution_id)
        return self.record

    def get_execution_progress(self, execution_id: str) -> _Progress:
        return _Progress(self.get_execution(execution_id))


class _ScheduledPipeline:
    def __init__(self) -> None:
        self.name = "scheduled-pipeline"
        self.scheduler: dict[str, object] = {"name": "slurm", "job_name": "operator-name"}
        self.records: list[_Record] = []
        self.read_index = 0
        self.last_read: _Record | None = None
        self.submit_calls: list[tuple[bool, bool, str]] = []
        self.cancel_calls = 0

    def submit(self, *, submit: bool, wait: bool, execution_id: str) -> _Handle:
        self.submit_calls.append((submit, wait, execution_id))
        submission = {
            "schema_version": "jarvis.scheduler.submission.v1",
            "execution_id": execution_id,
            "provider": "slurm",
            "script_path": "/tmp/submit.slurm",
            "scheduler_job_id": "24680",
            "scheduler_cluster": None,
            "identity_source": "scheduler_submit_api",
            "submitted": True,
            "reconciliation_marker": self.scheduler["job_name"],
        }
        base = _Record(
            execution_id=execution_id,
            pipeline_id=self.name,
            mode="scheduler",
            state="submitted",
            submitted=True,
            terminal=False,
            scheduler_provider="slurm",
            scheduler_native_id="24680",
            metadata={"submission": submission},
        )
        self.records = [
            base,
            replace(base, state="running", updated_at="2026-07-12T00:00:02Z"),
            replace(
                base,
                state="completed",
                terminal=True,
                return_code=0,
                updated_at="2026-07-12T00:00:03Z",
            ),
        ]
        return base.handle

    def get_execution(self, execution_id: str) -> _Record:
        if not self.records:
            raise FileNotFoundError(execution_id)
        index = min(self.read_index, len(self.records) - 1)
        self.read_index += 1
        self.last_read = self.records[index]
        return self.last_read

    def get_execution_progress(self, execution_id: str) -> _Progress:
        if self.last_read is None:
            raise FileNotFoundError(execution_id)
        return _Progress(self.last_read)

    def cancel(self) -> None:
        self.cancel_calls += 1


def test_direct_broker_keeps_execution_blocking_and_emits_native_terminal_state() -> None:
    pipeline = _DirectPipeline()
    records: list[dict[str, Any]] = []

    def append(record: dict[str, Any]) -> None:
        records.append(record)
        execution_record = record.get("execution_record")
        if isinstance(execution_record, dict):
            typed_execution_record = cast(dict[str, Any], execution_record)
            if typed_execution_record.get("state") == "running":
                pipeline.finish.set()

    run_native_jarvis_broker(
        pipeline,
        runtime_intent=_intent(scheduler_expected="unknown"),
        runtime_direct_proof="direct-proof",
        configured_scheduler_provider="external",
        append_runtime_record=append,
        sleep=_yielding_sleep,
    )

    assert pipeline.run_calls == [("jarvis_test_execution", True)]
    assert records[0]["schema_version"] == "jarvis.runtime.v1"
    assert records[0]["details"]["direct_execution_proof"] == "direct-proof"
    native_states = [
        record["execution_record"]["state"] for record in records if "execution_record" in record
    ]
    assert native_states == ["running", "completed"]
    assert records[-1]["progress"]["terminal"] is True


def test_direct_broker_rejects_a_returned_nonterminal_record_without_hanging() -> None:
    pipeline = _DirectPipeline(terminal=False)

    with pytest.raises(
        RuntimeError,
        match="ended without an authoritative terminal record",
    ):
        run_native_jarvis_broker(
            pipeline,
            runtime_intent=_intent(scheduler_expected=False),
            runtime_direct_proof="unused-proof",
            configured_scheduler_provider="external",
            append_runtime_record=lambda _record: None,
            sleep=_yielding_sleep,
        )

    assert pipeline.run_calls == [("jarvis_test_execution", True)]


def test_scheduler_broker_polls_jarvis_after_submit_and_never_cancels_by_default() -> None:
    pipeline = _ScheduledPipeline()
    records: list[dict[str, Any]] = []

    run_native_jarvis_broker(
        pipeline,
        runtime_intent=_intent(scheduler_expected=True),
        runtime_direct_proof="unused-proof",
        configured_scheduler_provider="slurm",
        append_runtime_record=records.append,
        sleep=_yielding_sleep,
    )

    assert pipeline.submit_calls == [(True, False, "jarvis_test_execution")]
    assert pipeline.scheduler["job_name"] == _MARKER
    assert pipeline.cancel_calls == 0
    assert [record["execution_record"]["state"] for record in records] == [
        "submitted",
        "running",
        "completed",
    ]
    assert all(record["execution_handle"]["scheduler_native_id"] == "24680" for record in records)


@pytest.mark.parametrize(
    ("pipeline_provider", "configured_provider", "error"),
    [
        ("slurm", "external", "slurm != external"),
        ("site-batch", "slurm", "site-batch != slurm"),
        ("site-batch", "site-batch", "only through slurm"),
    ],
)
def test_scheduler_broker_rejects_provider_before_any_submission(
    pipeline_provider: str,
    configured_provider: str,
    error: str,
) -> None:
    pipeline = _ScheduledPipeline()
    pipeline.scheduler["name"] = pipeline_provider
    records: list[dict[str, Any]] = []

    with pytest.raises(RuntimeError, match=error):
        run_native_jarvis_broker(
            pipeline,
            runtime_intent=_intent(scheduler_expected="unknown"),
            runtime_direct_proof="unused-proof",
            configured_scheduler_provider=configured_provider,
            append_runtime_record=records.append,
            sleep=_yielding_sleep,
        )

    assert pipeline.submit_calls == []
    assert pipeline.scheduler["job_name"] == "operator-name"
    assert len(records) == 1
    assert records[0]["scheduler_phase"] == "launch_refused"
    assert records[0]["terminal"]["terminal"] is True
    assert records[0]["details"]["scheduler_submission_attempted"] is False
    assert records[0]["details"]["scheduler_launch_refused"] is True


def test_broker_uses_one_credential_closed_runtime_and_no_relay_scheduler_polling() -> None:
    source = scheduled_jarvis_command(
        "slurm",
        python_bin="python",
        pipeline_path=Path("pipeline.yaml"),
    )[4]

    assert "from clio_relay.jarvis_execution import run_native_jarvis_broker" in source
    assert "provider_for_scheduler" not in source
    assert "SchedulerPhase" not in source
    assert "run_native_jarvis_broker(" in source
    assert endpoint_module.RUNTIME_SIDECAR_MAX_RECORD_BYTES == 5 * 1024 * 1024
    assert endpoint_module.RUNTIME_SIDECAR_MAX_TOTAL_BYTES == 64 * 1024 * 1024
    assert "RUNTIME_METADATA_MAX_RECORD_BYTES = 5 * 1024 * 1024" in source
    assert "RUNTIME_METADATA_MAX_TOTAL_BYTES = 64 * 1024 * 1024" in source

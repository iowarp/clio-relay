"""Tests for the ``jarvis-mcp-validate`` top-level command (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` alongside its extraction into
``src/clio_relay/cli_jarvis_mcp_validate.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command moves with the logic it exercises. Classified by
whether the test actually drives ``CliRunner().invoke(app, ["jarvis-mcp-
validate", ...])`` -- the many sibling tests that instead call a still
cli.py-resident JARVIS execution-query engine function directly (e.g.
``cli._run_post_run_jarvis_execution_query(...)``, ``cli.
_execute_jarvis_execution_query(...)``) as a plain unit test, with no
CliRunner invocation anywhere in the test body, stayed in
``tests/test_cli.py``: they exercise resident code, not this command's thin
wrapper (``cli_jarvis_mcp_validate.py``'s own docstring names that engine
and why it stays cli.py-resident for now).

**Patch-target parity.** Every test below patches a collaborator either on
its owner module directly (``mcp_stdio_validation``, ``remote_cli``,
``jarvis_mcp_validation``, all already module-attribute-imported in
``tests/test_cli.py``) or on ``cli`` itself for the engine functions/classes/
constants that stay cli.py-resident (``cli._run_post_run_jarvis_execution_
query``, ``cli._JarvisExecutionQueryAcceptance``, ``cli.
_JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA``, and similar) -- never through
a symbol this extraction actually moved, so the move needed no patch-target
changes at all; ``cli`` here is the same ``from clio_relay import cli``
bare-module import ``tests/test_cli.py`` already uses throughout.

**Shared test helpers.** ``_write_test_cluster``, ``_jarvis_resume_
observation``, and ``_jarvis_resume_runtime_metadata`` stay defined in
``tests/test_cli.py`` -- each also has remaining callers among the engine
unit tests that stayed there -- and are imported here rather than
duplicated, the same ``from tests.test_cli import (...)`` precedent every
prior command-group extraction in this campaign established.

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on, plus a session-teardown collaborator half none
of these tests exercise). Reproduced here as the env-var half only.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.cli_jarvis_dispatch as cli_jarvis_dispatch
import clio_relay.cli_jarvis_execution_run as cli_jarvis_execution_run
import clio_relay.cli_jarvis_execution_types as cli_jarvis_execution_types
import clio_relay.cli_jarvis_package_search as cli_jarvis_package_search
import clio_relay.cli_jarvis_remote_contract as cli_jarvis_remote_contract
import clio_relay.cli_jarvis_resume_checkpoint as cli_jarvis_resume_checkpoint
import clio_relay.cli_remote_worker_probe as cli_remote_worker_probe
import clio_relay.jarvis_mcp_validation as jarvis_mcp_validation
import clio_relay.mcp_stdio_validation as mcp_stdio_validation
import clio_relay.remote_cli as remote_cli
from clio_relay import cli
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, ObservationTimeoutError
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationCheck,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    new_live_validation_report,
    write_validation_report,
)
from tests.test_cli import (
    _jarvis_resume_observation,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _jarvis_resume_runtime_metadata,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _write_test_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only."""
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_cli_jarvis_mcp_preflight_failure_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    report_path = tmp_path / "jarvis-preflight-failed.json"

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "ares",
            "--arguments-json",
            "{}",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code != 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "jarvis-mcp.preflight"


def test_unobserved_query_checkpoint_resumes_after_multiday_delay_without_new_run(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Checkpoint age never turns a queued HPC execution into a failed or duplicate run."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    _write_test_cluster(tmp_path, name="test-cluster")
    selector: dict[str, object] = {
        "cluster": "test-cluster",
        "scheduler_cluster": "test-cluster",
        "pipeline_id": "pipeline",
        "execution_id": "execution",
        "scheduler_provider": "slurm",
        "scheduler_native_id": "4242",
        "last_query_job_id": None,
    }
    checkpoint: dict[str, Any] = {
        "schema_version": cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "phase": "execution_query",
        "observation_state": "not_observed",
        "profile": "user",
        "retry_selector": selector,
        "builder_inputs": {
            "cluster": "test-cluster",
            "scheduler_cluster": "test-cluster",
            "tool": "jarvis_run",
            "runtime_metadata": _jarvis_resume_runtime_metadata(
                state="submitted",
                scheduler_native_id="4242",
            ),
        },
        "lifecycle_observations": [],
    }
    report = new_live_validation_report(scenario="remote-mcp", cluster="test-cluster")
    old_time = datetime.now(UTC) - timedelta(days=30)
    report.started_at = old_time
    report.completed_at = old_time
    report.status = ValidationStatus.PENDING
    report.resources = [
        ValidationResource(
            kind="jarvis_execution",
            resource_id="execution",
            role="resumable_acceptance_workload",
            cluster="test-cluster",
            provider="slurm",
            state="observation_pending",
            metadata={
                "retry_selector": selector,
                "outcome": "observation_pending",
                "scheduler_action": "none",
                "relay_action": "retain",
                "resume_checkpoint": checkpoint,
            },
        )
    ]
    resume_path = tmp_path / "multiday-query-pending.json"
    write_validation_report(report, resume_path)
    query_calls: list[dict[str, object]] = []

    def still_pending(**kwargs: object) -> cli_jarvis_execution_types._JarvisExecutionQueryPending:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        query_calls.append(dict(kwargs))
        return cli_jarvis_execution_types._JarvisExecutionQueryPending(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster="test-cluster",
            pipeline_id="pipeline",
            execution_id="execution",
            selector=cast(dict[str, object], kwargs["retry_selector"]),
        )

    def forbid_new_run(**_kwargs: object) -> None:
        raise AssertionError("query-only resume must not dispatch jarvis_run")

    def execute_locally(_definition: ClusterDefinition) -> bool:
        return False

    monkeypatch.setattr(
        cli_jarvis_execution_run, "_run_post_run_jarvis_execution_query", still_pending
    )
    monkeypatch.setattr(mcp_stdio_validation, "run_packaged_mcp_stdio_session", forbid_new_run)
    monkeypatch.setattr(remote_cli, "should_execute_on_cluster", execute_locally)

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--resume-report",
            str(resume_path),
            "--wait-timeout-seconds",
            "5",
            "--poll-seconds",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert len(query_calls) == 1
    assert query_calls[0]["pipeline_id"] == "pipeline"
    assert query_calls[0]["execution_id"] == "execution"
    persisted = LiveValidationReport.model_validate_json(resume_path.read_text(encoding="utf-8"))
    assert persisted.status is ValidationStatus.PENDING
    persisted_selector = persisted.resources[0].metadata["retry_selector"]
    assert persisted_selector["execution_id"] == "execution"
    assert persisted.started_at == old_time


def test_initial_relay_dispatch_timeout_resumes_exact_job_without_duplicate_run(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Once jarvis_run returns a receipt, resume observes that job and never calls it again."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    _write_test_cluster(tmp_path, name="test-cluster")
    pending_path = tmp_path / "dispatch-pending.json"
    stdio_calls: list[dict[str, object]] = []
    dispatch_calls: list[str] = []

    def discover(**_kwargs: object) -> tuple[str, dict[str, Any], list[dict[str, Any]], bytes]:
        return "job-discovery", {}, [], b"{}"

    def persist_discovery(**_kwargs: object) -> None:
        return None

    package_search = cli_jarvis_package_search._JarvisPackageSearchAcceptance(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tools_list_response={},
        call_response={},
        call_job_id="job-search",
        call_status={},
        artifacts=[],
        mcp_result=None,
        provenance=None,
        initialize_response={},
        stdio_evidence={},
    )

    def search(**_kwargs: object) -> cli_jarvis_package_search._JarvisPackageSearchAcceptance:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        return package_search

    def run_session(**kwargs: object) -> SimpleNamespace:
        stdio_calls.append(dict(kwargs))
        call_response = {"result": {"structuredContent": {"job_id": "job-run"}}}
        return SimpleNamespace(
            tools_list_response={},
            tools_call_response=call_response,
            initialize_response={},
            evidence=lambda: {"call_job_id": "job-run", "transport": "stdio"},
        )

    def complete_dispatch(**kwargs: object) -> dict[str, Any]:
        checkpoint = cast(dict[str, Any], kwargs["checkpoint"])
        selector = cast(dict[str, object], checkpoint["retry_selector"])
        dispatch_calls.append(cast(str, selector["relay_job_id"]))
        assert selector["relay_job_id"] == "job-run"
        if len(dispatch_calls) == 1:
            raise ObservationTimeoutError("relay dispatch remains queued")
        return {
            **cast(dict[str, Any], checkpoint["builder_inputs"]),
            "call_status": {"terminal": True},
            "runtime_metadata": _jarvis_resume_runtime_metadata(
                state="submitted",
                scheduler_native_id="4242",
            ),
        }

    def build_report(**kwargs: Any) -> LiveValidationReport:
        report = new_live_validation_report(
            scenario="remote-mcp",
            cluster=cast(str, kwargs["cluster"]),
        )
        if kwargs["query_call_job_id"] == "":
            now = datetime.now(UTC)
            report.completed_at = now
            report.status = ValidationStatus.FAILED
            report.checks = [
                ValidationCheck(
                    check_id="remote-mcp.jarvis-call",
                    summary="relay dispatch reaches terminal observation",
                    status=ValidationStatus.FAILED,
                    started_at=report.started_at,
                    completed_at=now,
                    error="observation pending",
                )
            ]
            return report
        recorder = ValidationRecorder(report)
        with recorder.check("jarvis.complete", "same execution completed") as evidence:
            evidence.append(EvidenceReference(kind="test", excerpt="same durable execution"))
        recorder.finish()
        return report

    def terminal_query(
        **kwargs: object,
    ) -> cli_jarvis_execution_types._JarvisExecutionQueryAcceptance:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        pipeline_id = cast(str, kwargs["pipeline_id"])
        execution_id = cast(str, kwargs["execution_id"])
        return cli_jarvis_execution_types._JarvisExecutionQueryAcceptance(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster="test-cluster",
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            outcome="terminal",
            tools_list_response={},
            call_response={},
            call_job_id="job-query",
            call_status={},
            artifacts=[],
            mcp_result=None,
            provenance=None,
            initialize_response={},
            stdio_evidence={},
            lifecycle_observations=[
                _jarvis_resume_observation(
                    query_job_id="job-query",
                    state="completed",
                    terminal=True,
                    scheduler_native_id="4242",
                )
            ],
        )

    def execute_locally(_definition: ClusterDefinition) -> bool:
        return False

    monkeypatch.setattr(
        cli_jarvis_remote_contract, "_run_jarvis_remote_contract_discovery", discover
    )
    monkeypatch.setattr(
        cli_jarvis_remote_contract, "_persist_jarvis_remote_contract_discovery", persist_discovery
    )
    monkeypatch.setattr(cli_jarvis_package_search, "_run_jarvis_package_search_query", search)
    monkeypatch.setattr(mcp_stdio_validation, "run_packaged_mcp_stdio_session", run_session)
    monkeypatch.setattr(cli_jarvis_dispatch, "_complete_jarvis_run_dispatch", complete_dispatch)
    monkeypatch.setattr(
        cli_jarvis_execution_run, "_run_post_run_jarvis_execution_query", terminal_query
    )
    monkeypatch.setattr(jarvis_mcp_validation, "build_jarvis_mcp_validation_report", build_report)
    monkeypatch.setattr(remote_cli, "should_execute_on_cluster", execute_locally)

    initial = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--package-search-query",
            "paraview",
            "--arguments-json",
            '{"pipeline_id":"pipeline"}',
            "--wait-timeout-seconds",
            "5",
            "--poll-seconds",
            "1",
            "--report",
            str(pending_path),
        ],
    )

    assert initial.exit_code == 0, initial.output
    assert len(stdio_calls) == 1
    initial_arguments = cast(dict[str, object], stdio_calls[0]["arguments"])
    assert isinstance(initial_arguments["idempotency_key"], str)
    pending = LiveValidationReport.model_validate_json(pending_path.read_text(encoding="utf-8"))
    checkpoint = pending.resources[-1].metadata["resume_checkpoint"]
    assert checkpoint["phase"] == "jarvis_run_dispatch"
    assert checkpoint["retry_selector"]["relay_job_id"] == "job-run"

    resumed = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--resume-report",
            str(pending_path),
            "--wait-timeout-seconds",
            "5",
            "--poll-seconds",
            "1",
        ],
    )

    assert resumed.exit_code == 0, resumed.output
    assert len(stdio_calls) == 1
    assert dispatch_calls == ["job-run", "job-run"]
    final = LiveValidationReport.model_validate_json(pending_path.read_text(encoding="utf-8"))
    assert final.status is ValidationStatus.PASSED


def test_stdio_receipt_timeout_replays_only_the_same_idempotent_intent(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """An ambiguous stdio boundary may replay its key, but may not mint a new workload key."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    _write_test_cluster(tmp_path, name="test-cluster")
    pending_path = tmp_path / "intent-pending.json"
    observed_arguments: list[dict[str, object]] = []

    def discover(
        **_kwargs: object,
    ) -> tuple[str, dict[str, Any], list[dict[str, Any]], bytes]:
        return "job-discovery", {}, [], b"{}"

    def persist_discovery(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(
        cli_jarvis_remote_contract, "_run_jarvis_remote_contract_discovery", discover
    )
    monkeypatch.setattr(
        cli_jarvis_remote_contract, "_persist_jarvis_remote_contract_discovery", persist_discovery
    )
    package_search = cli_jarvis_package_search._JarvisPackageSearchAcceptance(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tools_list_response={},
        call_response={},
        call_job_id="job-search",
        call_status={},
        artifacts=[],
        mcp_result=None,
        provenance=None,
        initialize_response={},
        stdio_evidence={},
    )

    def search(**_kwargs: object) -> cli_jarvis_package_search._JarvisPackageSearchAcceptance:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        return package_search

    monkeypatch.setattr(cli_jarvis_package_search, "_run_jarvis_package_search_query", search)

    def run_session(**kwargs: object) -> SimpleNamespace:
        observed_arguments.append(cast(dict[str, object], kwargs["arguments"]))
        if len(observed_arguments) == 1:
            pre_dispatch = LiveValidationReport.model_validate_json(
                pending_path.read_text(encoding="utf-8")
            )
            assert (
                pre_dispatch.resources[0].metadata["resume_checkpoint"]["phase"]
                == "jarvis_run_intent"
            )
            raise ObservationTimeoutError("stdio response was not observed")
        return SimpleNamespace(
            tools_list_response={},
            tools_call_response={"result": {"structuredContent": {"job_id": "job-run"}}},
            initialize_response={},
            evidence=lambda: {"call_job_id": "job-run", "transport": "stdio"},
        )

    def still_queued(**_kwargs: object) -> None:
        raise ObservationTimeoutError("accepted relay job remains queued")

    def build_pending(**kwargs: Any) -> LiveValidationReport:
        report = new_live_validation_report(
            scenario="remote-mcp",
            cluster=cast(str, kwargs["cluster"]),
        )
        now = datetime.now(UTC)
        report.completed_at = now
        report.status = ValidationStatus.FAILED
        report.checks = [
            ValidationCheck(
                check_id="remote-mcp.jarvis-call",
                summary="relay dispatch reaches terminal observation",
                status=ValidationStatus.FAILED,
                started_at=report.started_at,
                completed_at=now,
                error="observation pending",
            )
        ]
        return report

    def execute_locally(_definition: ClusterDefinition) -> bool:
        return False

    monkeypatch.setattr(mcp_stdio_validation, "run_packaged_mcp_stdio_session", run_session)
    monkeypatch.setattr(cli_jarvis_dispatch, "_complete_jarvis_run_dispatch", still_queued)
    monkeypatch.setattr(jarvis_mcp_validation, "build_jarvis_mcp_validation_report", build_pending)
    monkeypatch.setattr(remote_cli, "should_execute_on_cluster", execute_locally)

    initial = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--package-search-query",
            "paraview",
            "--arguments-json",
            '{"pipeline_id":"pipeline"}',
            "--wait-timeout-seconds",
            "5",
            "--poll-seconds",
            "1",
            "--report",
            str(pending_path),
        ],
    )
    assert initial.exit_code == 0, initial.output
    first = LiveValidationReport.model_validate_json(pending_path.read_text(encoding="utf-8"))
    assert first.resources[0].metadata["resume_checkpoint"]["phase"] == "jarvis_run_intent"

    resumed = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--resume-report",
            str(pending_path),
            "--wait-timeout-seconds",
            "5",
            "--poll-seconds",
            "1",
        ],
    )

    assert resumed.exit_code == 0, resumed.output
    assert len(observed_arguments) == 2
    assert observed_arguments[0] == observed_arguments[1]
    assert observed_arguments[0]["idempotency_key"] == observed_arguments[1]["idempotency_key"]
    second = LiveValidationReport.model_validate_json(pending_path.read_text(encoding="utf-8"))
    second_checkpoint = second.resources[-1].metadata["resume_checkpoint"]
    assert second_checkpoint["phase"] == "jarvis_run_dispatch"
    assert second_checkpoint["retry_selector"]["relay_job_id"] == "job-run"


def test_jarvis_validation_rejects_unresumable_secret_arguments(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """A resume checkpoint never persists credentials needed to replay a workload call."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, name="test-cluster")
    report_path = tmp_path / "secret-rejected.json"

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--package-search-query",
            "paraview",
            "--arguments-json",
            '{"pipeline_id":"pipeline","api_token":"must-not-leak"}',
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 2
    assert "credential-redacted" in result.output
    assert "must-not-leak" not in result.output
    assert "must-not-leak" not in report_path.read_text(encoding="utf-8")


def test_jarvis_mcp_validate_resume_report_queries_exact_execution_without_new_run(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    cluster_path = tmp_path / ".clio-relay" / "clusters.json"
    cluster_path.parent.mkdir(parents=True)
    cluster_path.write_text(
        json.dumps(
            {
                "clusters": {
                    "test-cluster": ClusterDefinition(
                        name="test-cluster",
                        ssh_host="test-login",
                    ).model_dump(mode="json")
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    resume_path = tmp_path / "jarvis-pending.json"
    prior_observation = _jarvis_resume_observation(
        query_job_id="job-query-old",
        state="submitted",
        terminal=False,
        scheduler_native_id="4242",
    )
    checkpoint = {
        "schema_version": cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "phase": "execution_query",
        "observation_state": "observed",
        "profile": "user",
        "retry_selector": {
            "cluster": "test-cluster",
            "scheduler_cluster": "test-cluster",
            "pipeline_id": "pipeline",
            "execution_id": "execution",
            "scheduler_provider": "slurm",
            "scheduler_native_id": "4242",
            "last_query_job_id": "job-query-old",
        },
        "builder_inputs": {
            "cluster": "test-cluster",
            "scheduler_cluster": "test-cluster",
            "tool": "jarvis_run",
            "runtime_metadata": _jarvis_resume_runtime_metadata(
                state="submitted",
                scheduler_native_id="4242",
            ),
        },
        "lifecycle_observations": [prior_observation],
    }
    pending_report = new_live_validation_report(
        scenario="remote-mcp",
        cluster="test-cluster",
    )
    pending_report.status = ValidationStatus.PENDING
    pending_report.completed_at = datetime.now(UTC)
    pending_report.resources.append(
        ValidationResource(
            kind="jarvis_execution",
            resource_id="execution",
            role="resumable_acceptance_workload",
            cluster="test-cluster",
            provider="slurm",
            state="submitted",
            metadata={
                "retry_selector": checkpoint["retry_selector"],
                "resume_checkpoint": checkpoint,
            },
        )
    )
    write_validation_report(pending_report, resume_path)
    query_calls: list[tuple[str, str, str]] = []

    def query_exact_execution(
        *,
        cluster: str,
        definition: ClusterDefinition,
        queue: ClioCoreQueue,
        profile: str,
        pipeline_id: str,
        execution_id: str,
        retry_selector: dict[str, object] | None,
        wait_timeout_seconds: float,
        poll_seconds: float,
    ) -> cli_jarvis_execution_types._JarvisExecutionQueryAcceptance:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        del definition, queue, retry_selector, wait_timeout_seconds, poll_seconds
        query_calls.append((profile, pipeline_id, execution_id))
        terminal_observation = _jarvis_resume_observation(
            query_job_id="job-query-new",
            state="completed",
            terminal=True,
            scheduler_native_id="4242",
        )
        return cli_jarvis_execution_types._JarvisExecutionQueryAcceptance(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster=cluster,
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            outcome="terminal",
            tools_list_response={},
            call_response={},
            call_job_id="job-query-new",
            call_status={},
            artifacts=[],
            mcp_result=None,
            provenance=None,
            initialize_response={},
            stdio_evidence={},
            lifecycle_observations=[terminal_observation],
        )

    builder_calls: list[dict[str, Any]] = []

    def build_report(**kwargs: Any) -> LiveValidationReport:
        builder_calls.append(kwargs)
        report = new_live_validation_report(
            scenario="remote-mcp",
            cluster="test-cluster",
        )
        recorder = ValidationRecorder(report)
        with recorder.check("jarvis.resume", "resume exact execution") as evidence:
            evidence.append(EvidenceReference(kind="test", excerpt="same execution"))
        recorder.finish()
        return report

    def forbid_new_run(**_kwargs: object) -> None:
        raise AssertionError("resume must not dispatch jarvis_run")

    def execute_locally(_definition: ClusterDefinition) -> bool:
        return False

    monkeypatch.setattr(
        cli_jarvis_execution_run, "_run_post_run_jarvis_execution_query", query_exact_execution
    )
    monkeypatch.setattr(jarvis_mcp_validation, "build_jarvis_mcp_validation_report", build_report)
    monkeypatch.setattr(mcp_stdio_validation, "run_packaged_mcp_stdio_session", forbid_new_run)
    monkeypatch.setattr(remote_cli, "should_execute_on_cluster", execute_locally)

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--resume-report",
            str(resume_path),
            "--wait-timeout-seconds",
            "5",
            "--poll-seconds",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert query_calls == [("user", "pipeline", "execution")]
    assert len(builder_calls) == 1
    assert [
        observation["state"] for observation in builder_calls[0]["query_lifecycle_observations"]
    ] == ["submitted", "completed"]
    completed = LiveValidationReport.model_validate_json(resume_path.read_text(encoding="utf-8"))
    assert completed.status is ValidationStatus.PASSED


def test_jarvis_resume_pending_returns_before_release_provenance_observation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A durable pending workload survives an ancillary worker-info failure boundary."""
    cluster_path = tmp_path / ".clio-relay" / "clusters.json"
    cluster_path.parent.mkdir(parents=True)
    cluster_path.write_text(
        json.dumps(
            {
                "clusters": {
                    "test-cluster": ClusterDefinition(
                        name="test-cluster",
                        ssh_host="test-login",
                    ).model_dump(mode="json")
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    resume_path = tmp_path / "jarvis-pending.json"
    prior_observation = _jarvis_resume_observation(
        query_job_id="job-query-old",
        state="submitting",
        terminal=False,
        scheduler_native_id=None,
        scheduler_cluster=None,
    )
    selector = {
        "cluster": "test-cluster",
        "scheduler_cluster": None,
        "pipeline_id": "pipeline",
        "execution_id": "execution",
        "scheduler_provider": "slurm",
        "scheduler_native_id": None,
        "last_query_job_id": "job-query-old",
    }
    checkpoint = {
        "schema_version": cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "phase": "execution_query",
        "observation_state": "observed",
        "profile": "user",
        "retry_selector": selector,
        "builder_inputs": {
            "cluster": "test-cluster",
            "scheduler_cluster": None,
            "tool": "jarvis_run",
            "runtime_metadata": _jarvis_resume_runtime_metadata(
                state="submitting",
                scheduler_native_id=None,
                scheduler_cluster=None,
            ),
        },
        "lifecycle_observations": [prior_observation],
    }
    pending_report = new_live_validation_report(scenario="remote-mcp", cluster="test-cluster")
    pending_report.status = ValidationStatus.PENDING
    pending_report.completed_at = datetime.now(UTC)
    pending_report.resources.append(
        ValidationResource(
            kind="jarvis_execution",
            resource_id="execution",
            role="resumable_acceptance_workload",
            cluster="test-cluster",
            provider="slurm",
            state="submitting",
            metadata={"retry_selector": selector, "resume_checkpoint": checkpoint},
        )
    )
    write_validation_report(pending_report, resume_path)

    def query_exact_execution(
        **kwargs: object,
    ) -> cli_jarvis_execution_types._JarvisExecutionQueryAcceptance:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        observation = _jarvis_resume_observation(
            query_job_id="job-query-new",
            state="submitted",
            terminal=False,
            scheduler_native_id="4242",
            scheduler_cluster="linux",
        )
        return cli_jarvis_execution_types._JarvisExecutionQueryAcceptance(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster=cast(str, kwargs["cluster"]),
            pipeline_id=cast(str, kwargs["pipeline_id"]),
            execution_id=cast(str, kwargs["execution_id"]),
            outcome="observation_unknown",
            tools_list_response={},
            call_response={},
            call_job_id="job-query-new",
            call_status={},
            artifacts=[],
            mcp_result=None,
            provenance=None,
            initialize_response={},
            stdio_evidence={},
            lifecycle_observations=[observation],
        )

    builder_calls: list[dict[str, Any]] = []

    def build_pending_report(**kwargs: Any) -> LiveValidationReport:
        builder_calls.append(kwargs)
        report = new_live_validation_report(scenario="remote-mcp", cluster="test-cluster")
        now = datetime.now(UTC)
        report.status = ValidationStatus.FAILED
        report.completed_at = now
        report.checks = [
            ValidationCheck(
                check_id="remote-mcp.jarvis-live-progress",
                summary="execution lifecycle remains nonterminal",
                status=ValidationStatus.FAILED,
                started_at=now,
                completed_at=now,
                evidence=[
                    EvidenceReference(
                        kind="test",
                        excerpt="coherent pending lifecycle",
                        metadata={
                            "assertions": {
                                "observation_count_bounded": True,
                                "query_identities_coherent": True,
                                "scheduler_identity_optional_coherent_and_stable": True,
                                "lifecycle_prefix_coherent": True,
                                "package_progress_nonregressing": True,
                            }
                        },
                    )
                ],
                error="execution remains submitted",
            ),
            ValidationCheck(
                check_id="remote-mcp.jarvis-execution-query",
                summary="execution query remains resumable",
                status=ValidationStatus.FAILED,
                started_at=now,
                completed_at=now,
                evidence=[
                    EvidenceReference(
                        kind="test",
                        excerpt="coherent resumable query",
                        metadata={
                            "assertions": {
                                "local_query_surface_verified": True,
                                "server_artifact_binding_verified": True,
                                "resumable_query_job_verified": True,
                                "resumable_result_transport_verified": True,
                                "resumable_result_envelope_verified": True,
                                "resumable_identity_coherent": True,
                                "resumable_lifecycle_coherent": True,
                                "resumable_runner_semantic_validation_verified": True,
                            }
                        },
                    )
                ],
                error="artifact page is not terminal yet",
            ),
        ]
        return report

    worker_info_calls: list[str] = []

    def fail_worker_info(_definition: ClusterDefinition) -> dict[str, object]:
        worker_info_calls.append("called")
        raise AssertionError("pending resume must not require release provenance")

    def execute_remotely(_definition: ClusterDefinition) -> bool:
        return True

    def forbid_new_run(**_kwargs: object) -> None:
        raise AssertionError("resume must not submit")

    monkeypatch.setattr(
        cli_jarvis_execution_run, "_run_post_run_jarvis_execution_query", query_exact_execution
    )
    monkeypatch.setattr(
        jarvis_mcp_validation, "build_jarvis_mcp_validation_report", build_pending_report
    )
    monkeypatch.setattr(remote_cli, "should_execute_on_cluster", execute_remotely)
    monkeypatch.setattr(cli_remote_worker_probe, "_remote_worker_info", fail_worker_info)
    monkeypatch.setattr(mcp_stdio_validation, "run_packaged_mcp_stdio_session", forbid_new_run)

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--resume-report",
            str(resume_path),
            "--wait-timeout-seconds",
            "5",
            "--poll-seconds",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert worker_info_calls == []
    assert len(builder_calls) == 1
    assert builder_calls[0]["scheduler_cluster"] == "linux"
    assert [
        observation["state"] for observation in builder_calls[0]["query_lifecycle_observations"]
    ] == ["submitting", "submitted"]
    persisted = LiveValidationReport.model_validate_json(resume_path.read_text(encoding="utf-8"))
    assert persisted.status is ValidationStatus.PENDING
    resource = next(item for item in persisted.resources if item.kind == "jarvis_execution")
    persisted_checkpoint = cast(dict[str, Any], resource.metadata["resume_checkpoint"])
    assert persisted_checkpoint["retry_selector"]["cluster"] == "test-cluster"
    assert persisted_checkpoint["retry_selector"]["scheduler_cluster"] == "linux"
    assert persisted_checkpoint["builder_inputs"]["scheduler_cluster"] == "linux"


def test_jarvis_resume_query_failure_preserves_pending_checkpoint(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A transient resume failure writes separate evidence and preserves its selector."""
    cluster_path = tmp_path / ".clio-relay" / "clusters.json"
    cluster_path.parent.mkdir(parents=True)
    cluster_path.write_text(
        json.dumps(
            {
                "clusters": {
                    "test-cluster": ClusterDefinition(
                        name="test-cluster",
                        ssh_host="test-login",
                    ).model_dump(mode="json")
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    resume_path = tmp_path / "jarvis-pending.json"
    checkpoint = {
        "schema_version": cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "phase": "execution_query",
        "observation_state": "observed",
        "profile": "user",
        "retry_selector": {
            "cluster": "test-cluster",
            "scheduler_cluster": "test-cluster",
            "pipeline_id": "pipeline",
            "execution_id": "execution",
            "scheduler_provider": "slurm",
            "scheduler_native_id": "4242",
            "last_query_job_id": "job-query-old",
        },
        "builder_inputs": {
            "cluster": "test-cluster",
            "scheduler_cluster": "test-cluster",
            "tool": "jarvis_run",
            "runtime_metadata": _jarvis_resume_runtime_metadata(
                state="submitted",
                scheduler_native_id="4242",
            ),
        },
        "lifecycle_observations": [
            _jarvis_resume_observation(
                query_job_id="job-query-old",
                state="submitted",
                terminal=False,
                scheduler_native_id="4242",
            )
        ],
    }
    pending_report = new_live_validation_report(
        scenario="remote-mcp",
        cluster="test-cluster",
    )
    pending_report.status = ValidationStatus.PENDING
    pending_report.completed_at = datetime.now(UTC)
    pending_report.resources.append(
        ValidationResource(
            kind="jarvis_execution",
            resource_id="execution",
            role="resumable_acceptance_workload",
            cluster="test-cluster",
            provider="slurm",
            state="submitted",
            metadata={
                "retry_selector": checkpoint["retry_selector"],
                "resume_checkpoint": checkpoint,
            },
        )
    )
    write_validation_report(pending_report, resume_path)
    original = resume_path.read_bytes()

    def fail_query(**_kwargs: object) -> None:
        raise TimeoutError("temporary relay observation timeout")

    monkeypatch.setattr(
        cli_jarvis_execution_run, "_run_post_run_jarvis_execution_query", fail_query
    )

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--resume-report",
            str(resume_path),
            "--wait-timeout-seconds",
            "5",
            "--poll-seconds",
            "1",
        ],
    )

    assert result.exit_code == 1
    assert resume_path.read_bytes() == original
    failure_paths = list(tmp_path.glob("jarvis-pending.resume-failure-*.json"))
    assert len(failure_paths) == 1
    failure = LiveValidationReport.model_validate_json(failure_paths[0].read_text(encoding="utf-8"))
    assert failure.status is ValidationStatus.FAILED
    loaded = cli_jarvis_resume_checkpoint._load_jarvis_validation_resume_checkpoint(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        resume_path,
        cluster="test-cluster",
    )
    assert loaded["retry_selector"]["scheduler_native_id"] == "4242"

    tampered_path = tmp_path / "jarvis-tampered.json"
    tampered = LiveValidationReport.model_validate_json(original)
    resource = tampered.resources[0]
    resource.metadata["retry_selector"]["execution_id"] = "different-execution"
    resource.metadata["resume_checkpoint"]["retry_selector"]["execution_id"] = "different-execution"
    write_validation_report(tampered, tampered_path)
    with pytest.raises(
        ConfigurationError,
        match="resume identity is invalid|observation identity changed",
    ):
        cli_jarvis_resume_checkpoint._load_jarvis_validation_resume_checkpoint(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            tampered_path,
            cluster="test-cluster",
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_record",
        "record_identity",
        "state",
        "terminal",
        "return_code",
        "missing_progress",
        "progress_identity",
        "integrity_marker",
        "gap_marker",
        "coordinated_cluster",
        "coordinated_mode",
        "coordinated_provider",
        "coordinated_pipeline",
        "coordinated_execution",
        "runtime_missing_native_execution",
        "runtime_missing_handle",
        "runtime_missing_record",
        "runtime_missing_progress",
        "runtime_handle_schema",
        "runtime_handle_native_id",
        "runtime_record_native_id",
        "runtime_progress_execution",
        "runtime_progress_schema",
        "runtime_terminal_state",
    ],
)
def test_jarvis_resume_rejects_tampered_checkpoint_before_remote_query(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    mutation: str,
) -> None:
    """Checkpoint-native documents are an admission boundary, not later report evidence."""
    cluster_path = tmp_path / ".clio-relay" / "clusters.json"
    cluster_path.parent.mkdir(parents=True)
    cluster_path.write_text(
        json.dumps(
            {
                "clusters": {
                    "test-cluster": ClusterDefinition(
                        name="test-cluster",
                        ssh_host="test-login",
                    ).model_dump(mode="json")
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    observation = _jarvis_resume_observation(
        query_job_id="job-query-old",
        state="submitted",
        terminal=False,
        scheduler_native_id="4242",
    )
    if mutation == "missing_record":
        observation.pop("execution_record")
    elif mutation == "record_identity":
        record = cast(dict[str, Any], observation["execution_record"])
        record["execution_id"] = "different-execution"
    elif mutation == "state":
        observation["state"] = "running"
    elif mutation == "terminal":
        observation["terminal"] = True
    elif mutation == "return_code":
        record = cast(dict[str, Any], observation["execution_record"])
        record["return_code"] = 1
    elif mutation == "missing_progress":
        observation.pop("progress")
    elif mutation == "progress_identity":
        progress = cast(dict[str, Any], observation["progress"])
        progress["execution_id"] = "different-execution"
    elif mutation == "integrity_marker":
        observation["relay_query_integrity"] = {}
    elif mutation == "gap_marker":
        observation["relay_query_verified_gap"] = {}
    elif mutation in {"coordinated_cluster", "coordinated_mode", "coordinated_provider"}:
        field, value = {
            "coordinated_cluster": ("cluster", "different-cluster"),
            "coordinated_mode": ("mode", "direct"),
            "coordinated_provider": ("scheduler_provider", "pbs"),
        }[mutation]
        for document_name in ("execution_handle", "execution_record"):
            document = cast(dict[str, Any], observation[document_name])
            document[field] = value
    elif mutation.startswith("runtime_"):
        pass
    else:
        field = "pipeline_id" if mutation == "coordinated_pipeline" else "execution_id"
        value = (
            "different-pipeline" if mutation == "coordinated_pipeline" else "different-execution"
        )
        observation[field] = value
        for document_name in ("execution_handle", "execution_record", "progress"):
            document = cast(dict[str, Any], observation[document_name])
            document[field] = value
    selector = {
        "cluster": "test-cluster",
        "scheduler_cluster": "test-cluster",
        "pipeline_id": "pipeline",
        "execution_id": "execution",
        "scheduler_provider": "slurm",
        "scheduler_native_id": "4242",
        "last_query_job_id": "job-query-old",
    }
    checkpoint = {
        "schema_version": cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "phase": "execution_query",
        "observation_state": "observed",
        "profile": "user",
        "retry_selector": selector,
        "builder_inputs": {
            "cluster": "test-cluster",
            "scheduler_cluster": "test-cluster",
            "tool": "jarvis_run",
            "runtime_metadata": _jarvis_resume_runtime_metadata(
                state="submitted",
                scheduler_native_id="4242",
            ),
        },
        "lifecycle_observations": [observation],
    }
    if mutation.startswith("runtime_"):
        builder = cast(dict[str, Any], checkpoint["builder_inputs"])
        runtime = cast(dict[str, Any], builder["runtime_metadata"])
        details = cast(dict[str, Any], runtime["details"])
        native = cast(dict[str, Any], details["native_execution"])
        if mutation == "runtime_missing_native_execution":
            details.pop("native_execution")
        elif mutation in {
            "runtime_missing_handle",
            "runtime_missing_record",
            "runtime_missing_progress",
        }:
            native.pop(
                {
                    "runtime_missing_handle": "execution_handle",
                    "runtime_missing_record": "execution_record",
                    "runtime_missing_progress": "progress",
                }[mutation]
            )
        elif mutation == "runtime_terminal_state":
            terminal = cast(dict[str, Any], runtime["terminal"])
            terminal["state"] = "running"
        else:
            document_name, field, value = {
                "runtime_handle_schema": (
                    "execution_handle",
                    "schema_version",
                    "jarvis.execution.handle.v0",
                ),
                "runtime_handle_native_id": (
                    "execution_handle",
                    "scheduler_native_id",
                    "9999",
                ),
                "runtime_record_native_id": (
                    "execution_record",
                    "scheduler_native_id",
                    "9999",
                ),
                "runtime_progress_execution": (
                    "progress",
                    "execution_id",
                    "different-execution",
                ),
                "runtime_progress_schema": (
                    "progress",
                    "schema_version",
                    "jarvis.execution.progress.v0",
                ),
            }[mutation]
            document = cast(dict[str, Any], native[document_name])
            document[field] = value
    pending_report = new_live_validation_report(
        scenario="remote-mcp",
        cluster="test-cluster",
    )
    pending_report.status = ValidationStatus.PENDING
    pending_report.completed_at = datetime.now(UTC)
    pending_report.resources.append(
        ValidationResource(
            kind="jarvis_execution",
            resource_id="execution",
            role="resumable_acceptance_workload",
            cluster="test-cluster",
            provider="slurm",
            state=str(observation.get("state")),
            metadata={"retry_selector": selector, "resume_checkpoint": checkpoint},
        )
    )
    resume_path = tmp_path / f"tampered-{mutation}.json"
    write_validation_report(pending_report, resume_path)
    query_calls: list[str] = []

    def forbid_query(**_kwargs: object) -> None:
        query_calls.append("called")
        raise AssertionError("tampered checkpoint reached a remote query")

    monkeypatch.setattr(
        cli_jarvis_execution_run, "_run_post_run_jarvis_execution_query", forbid_query
    )

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--resume-report",
            str(resume_path),
        ],
    )

    assert result.exit_code == 1
    assert query_calls == []

from __future__ import annotations

import hashlib
import json
import subprocess
import threading
import time
import urllib.request
from base64 import b64encode
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

from clio_relay.browser_gateway import BrowserAttachmentGrant, BrowserDetachmentResult
from clio_relay.cluster_config import ClusterDefinition, LiveTestConfig
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_service_runtime import JarvisServiceRuntimeHandoff
from clio_relay.live_acceptance import (
    CommandRunner,
    LiveAcceptanceOptions,
    SecureRuntimeHttpEvidence,
    SecureRuntimeProbeConfig,
    SecureRuntimeProtocolAdapter,
    _AcceptanceObservationPending,  # pyright: ignore[reportPrivateUsage]
    _assert_progress_adapter,  # pyright: ignore[reportPrivateUsage]
    _assert_secret_free_document,  # pyright: ignore[reportPrivateUsage]
    _browser_json_observation,  # pyright: ignore[reportPrivateUsage]
    _browser_sse_observation,  # pyright: ignore[reportPrivateUsage]
    _BrowserHttpRequestError,  # pyright: ignore[reportPrivateUsage]
    _expected_progress_adapter,  # pyright: ignore[reportPrivateUsage]
    _expected_progress_package,  # pyright: ignore[reportPrivateUsage]
    _find_agent_child_job,  # pyright: ignore[reportPrivateUsage]
    _http_json,  # pyright: ignore[reportPrivateUsage]
    _packaged_mcp_acceptance_evidence,  # pyright: ignore[reportPrivateUsage]
    _require_secure_runtime_control_capacity,  # pyright: ignore[reportPrivateUsage]
    _secure_runtime_probe_config,  # pyright: ignore[reportPrivateUsage]
    _select_secure_runtime_handoff,  # pyright: ignore[reportPrivateUsage]
    _validated_secure_runtime_pending_bind,  # pyright: ignore[reportPrivateUsage]
    _verify_cluster_deployment,  # pyright: ignore[reportPrivateUsage]
    _verify_live_package_progress,  # pyright: ignore[reportPrivateUsage]
    _verify_runtime_metadata_artifact,  # pyright: ignore[reportPrivateUsage]
    _verify_secure_runtime_acceptance,  # pyright: ignore[reportPrivateUsage]
    _wait_for_live_structured_runtime_metadata,  # pyright: ignore[reportPrivateUsage]
    run_live_acceptance,
)
from clio_relay.mcp_stdio_validation import PackagedMcpStdioSession
from clio_relay.models import (
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
)
from clio_relay.service_runtime import ServiceRuntimeStartResult, ServiceRuntimeStopResult
from clio_relay.session_lifecycle import CleanupResource
from clio_relay.validation_report import (
    TransportCleanupResourceEvidence,
    TransportProbeEvidence,
    ValidationRecorder,
    ValidationStatus,
    load_validation_report,
    new_live_validation_report,
    transport_probe_evidence_line,
)


class _HttpResponse:
    def __enter__(self) -> _HttpResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return b'{"ok": true}'


def _collection_page(record_key: str, records: list[dict[str, object]]) -> dict[str, object]:
    """Render one complete exact-family CLI page for remote acceptance fakes."""
    return {
        record_key: records,
        "cursor": 1,
        "limit": 500,
        "next_cursor": None,
        "total": len(records),
    }


def _provider_progress_record(
    *,
    job_id: str = "job_test",
    acceptance_validated: bool = True,
    prediction_status: str = "observed",
) -> dict[str, object]:
    """Build one durable worker/provider attestation for acceptance tests."""
    return {
        "current": 3.0,
        "metadata": {
            "adapter": "site-progress",
            "source": "jarvis_package",
            "package_name": "site.simulation",
            "package_version": "test-plugin",
            "run_id": job_id,
            "execution_id": job_id,
            "provider_entry_point": "site-progress",
            "provider_entry_point_value": ("tests.plugin_fakes:site_progress_adapter_from_package"),
            "provider_distribution": "site-progress-plugin",
            "provider_distribution_version": "3.4.5",
            "provider_source_authority": "jarvis_stdout_fallback",
            "application_profile": "site-stack",
            "provider_validated": True,
            "acceptance_validated": acceptance_validated,
            "prediction_status": prediction_status,
            "eta_seconds": 1.0,
        },
    }


_LIVE_SOURCE_JOB_ID = "job_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def _structured_live_runtime_metadata() -> dict[str, object]:
    """Build one valid nonterminal JARVIS-owned runtime observation."""
    return {
        "schema_version": "clio-relay.jarvis-runtime.v1",
        "source": "jarvis_mcp",
        "execution_id": "execution-live-41",
        "pipeline_id": "pipeline-live-41",
        "scheduler_provider": "slurm",
        "scheduler_job_id": "22064",
        "terminal": {"state": "running", "terminal": False},
        "field_sources": {
            "execution_id": "jarvis_mcp",
            "pipeline_id": "jarvis_mcp",
            "scheduler_provider": "jarvis_mcp",
            "scheduler_job_id": "jarvis_mcp",
        },
    }


def _live_source_status(
    state: JobState,
    *,
    runtime_metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    """Render the supported remote ``job status`` envelope for a source run."""
    metadata: dict[str, object] = {}
    if runtime_metadata is not None:
        metadata["runtime_metadata"] = runtime_metadata
    job = RelayJob(
        job_id=_LIVE_SOURCE_JOB_ID,
        cluster="test-cluster",
        kind=JobKind.JARVIS,
        state=state,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="secure-runtime-live-source",
        metadata=metadata,
    )
    return {
        "job": job.model_dump(mode="json"),
        "relay_queue": {"state": state.value},
        "scheduler": [],
        "terminal": state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED},
    }


def test_transport_http_client_sends_exact_owned_session_binding(
    monkeypatch: MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    def urlopen(request: urllib.request.Request, *, timeout: float) -> _HttpResponse:
        assert timeout == 5
        captured.update({name.lower(): value for name, value in request.header_items()})
        return _HttpResponse()

    monkeypatch.setattr("clio_relay.live_acceptance.urllib.request.urlopen", urlopen)

    assert _http_json(
        "http://127.0.0.1:18000",
        "POST",
        "/jobs/jarvis",
        api_token="api-token",
        owner_session_id="desktop-session-1",
        session_generation_id="generation-1",
        body={"cluster": "ares"},
        timeout_seconds=5,
    ) == {"ok": True}
    assert captured["authorization"] == "Bearer api-token"
    assert captured["x-clio-relay-owner-session-id"] == "desktop-session-1"
    assert captured["x-clio-relay-session-generation-id"] == "generation-1"

    with pytest.raises(ValueError, match="must be provided together"):
        _http_json(
            "http://127.0.0.1:18000",
            "POST",
            "/jobs/jarvis",
            api_token="api-token",
            owner_session_id="desktop-session-1",
            timeout_seconds=5,
        )


def test_live_acceptance_requires_configured_workload() -> None:
    with pytest.raises(ConfigurationError, match="live-test requires"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            )
        )


def test_cluster_deployment_verifier_requires_linger_enabled_and_active() -> None:
    """Release evidence cannot be emitted without all three persistence proofs."""
    observed: list[str] = []

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        observed.append(command[-1])
        return subprocess.CompletedProcess(
            command,
            78,
            stdout=b"",
            stderr=b"persistent worker requires systemd user lingering (Linger=yes)",
        )

    with pytest.raises(RelayError, match="requires systemd user lingering"):
        _verify_cluster_deployment(
            ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            runner=fake_runner,
            expected_artifact_sha256=None,
            expected_install_source=None,
        )

    assert len(observed) == 1
    assert 'loginctl show-user "$relay_user" -p Linger --value' in observed[0]
    assert "systemctl --user is-enabled clio-relay-worker-test-cluster.service" in observed[0]
    assert "systemctl --user is-active clio-relay-worker-test-cluster.service" in observed[0]
    assert observed[0].index("is-enabled") < observed[0].index("is-active")
    assert observed[0].index("is-active") < observed[0].index("endpoint worker-info")


def test_live_acceptance_reports_structured_runtime_metadata() -> None:
    runtime_metadata = {
        "schema_version": "clio-relay.jarvis-runtime.v1",
        "source": "jarvis_mcp",
        "scheduler_provider": "slurm",
        "scheduler_job_id": "21813",
        "field_sources": {
            "scheduler_provider": "jarvis_mcp",
            "scheduler_job_id": "jarvis_mcp",
        },
    }

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        assert "read-artifact artifact_runtime" in command[-1]
        return _completed(
            command,
            json.dumps(
                {
                    "encoding": "base64",
                    "data": b64encode(json.dumps(runtime_metadata).encode()).decode(),
                }
            ),
        )

    lines: list[str] = []
    _verify_runtime_metadata_artifact(
        ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        [{"artifact_id": "artifact_runtime", "kind": "runtime_metadata"}],
        line_prefix="acceptance",
        lines=lines,
        runner=fake_runner,
    )

    assert "acceptance.runtime_metadata_artifact=artifact_runtime" in lines
    assert "acceptance.runtime_metadata_source=jarvis_mcp" in lines
    assert "acceptance.structured_runtime_metadata=ok" in lines
    assert "acceptance.runtime_scheduler_provider=slurm" in lines
    assert "acceptance.runtime_scheduler_job_id=21813" in lines
    assert "acceptance.runtime_scheduler_job_id_source=jarvis_mcp" in lines
    assert "acceptance.structured_runtime_scheduler_identity=ok" in lines


def test_live_acceptance_does_not_mark_legacy_metadata_as_structured() -> None:
    runtime_metadata = {
        "schema_version": "clio-relay.jarvis-runtime.v1",
        "source": "legacy_stdout",
        "scheduler_provider": "slurm",
        "scheduler_job_id": "21813",
        "field_sources": {"scheduler_job_id": "legacy_stdout"},
    }

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        return _completed(
            command,
            json.dumps(
                {
                    "encoding": "base64",
                    "data": b64encode(json.dumps(runtime_metadata).encode()).decode(),
                }
            ),
        )

    lines: list[str] = []
    _verify_runtime_metadata_artifact(
        ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        [{"artifact_id": "artifact_runtime", "kind": "runtime_metadata"}],
        line_prefix="acceptance",
        lines=lines,
        runner=fake_runner,
    )

    assert "acceptance.structured_runtime_metadata=ok" not in lines
    assert "acceptance.structured_runtime_scheduler_identity=ok" not in lines
    assert "runtime_metadata.compatibility=acceptance:legacy_fallback" in lines


def test_live_acceptance_does_not_mark_untrusted_metadata_as_structured() -> None:
    runtime_metadata = {
        "schema_version": "clio-relay.jarvis-runtime.v1",
        "source": "untrusted_compatibility",
        "scheduler_provider": "slurm",
        "scheduler_job_id": "21813",
        "field_sources": {
            "scheduler_provider": "untrusted_compatibility",
            "scheduler_job_id": "untrusted_compatibility",
        },
    }

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        return _completed(
            command,
            json.dumps(
                {
                    "encoding": "base64",
                    "data": b64encode(json.dumps(runtime_metadata).encode()).decode(),
                }
            ),
        )

    lines: list[str] = []
    structured = _verify_runtime_metadata_artifact(
        ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        [{"artifact_id": "artifact_runtime", "kind": "runtime_metadata"}],
        line_prefix="acceptance",
        lines=lines,
        runner=fake_runner,
    )

    assert structured is not None and structured.structured is False
    assert "acceptance.structured_runtime_metadata=ok" not in lines
    assert "acceptance.structured_runtime_scheduler_identity=ok" not in lines
    assert "runtime_metadata.compatibility=acceptance:untrusted_compatibility" in lines


def test_secure_runtime_acceptance_polls_running_source_metadata() -> None:
    """A live service probe starts from authoritative metadata while its job runs."""
    commands: list[str] = []

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        commands.append(command[-1])
        return _completed(
            command,
            json.dumps(
                _live_source_status(
                    JobState.RUNNING,
                    runtime_metadata=_structured_live_runtime_metadata(),
                )
            ),
        )

    lines: list[str] = []
    result = _wait_for_live_structured_runtime_metadata(
        ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        _LIVE_SOURCE_JOB_ID,
        line_prefix="acceptance",
        lines=lines,
        timeout_seconds=5,
        poll_seconds=0.01,
        runner=fake_runner,
    )

    assert result.structured is True
    assert result.document["execution_id"] == "execution-live-41"
    assert "acceptance.job_state=running" in lines
    assert "acceptance.structured_runtime_metadata=ok" in lines
    assert "acceptance.source_job_retained=ok" in lines
    assert len(commands) == 1
    assert f"job status {_LIVE_SOURCE_JOB_ID}" in commands[0]


def test_secure_runtime_acceptance_fails_if_source_job_fails_before_metadata() -> None:
    """A failed source job cannot be mistaken for a not-yet-ready live service."""

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        return _completed(command, json.dumps(_live_source_status(JobState.FAILED)))

    with pytest.raises(RelayError, match="source job failed before structured runtime metadata"):
        _wait_for_live_structured_runtime_metadata(
            ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            _LIVE_SOURCE_JOB_ID,
            line_prefix="acceptance",
            lines=[],
            timeout_seconds=5,
            poll_seconds=0.01,
            runner=fake_runner,
        )


def test_secure_runtime_acceptance_metadata_poll_is_bounded() -> None:
    """Missing live metadata exhausts the caller's existing acceptance timeout."""
    calls = 0

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        del input
        calls += 1
        return _completed(command, json.dumps(_live_source_status(JobState.RUNNING)))

    with pytest.raises(
        RelayError,
        match="timed out waiting for structured runtime metadata",
    ) as pending:
        _wait_for_live_structured_runtime_metadata(
            ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            _LIVE_SOURCE_JOB_ID,
            line_prefix="acceptance",
            lines=[],
            timeout_seconds=0,
            poll_seconds=0.01,
            runner=fake_runner,
        )

    assert calls == 1
    pending_observation = cast(_AcceptanceObservationPending, pending.value)
    assert pending_observation.phase == "secure_runtime_metadata"
    assert pending_observation.identifiers == {"primary_job_id": _LIVE_SOURCE_JOB_ID}


def test_secure_runtime_acceptance_rejects_unsupported_metadata_schema() -> None:
    """A model-shaped document cannot bypass the exact runtime schema pin."""
    metadata = _structured_live_runtime_metadata()
    metadata["schema_version"] = "unsupported.runtime.v99"

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        return _completed(
            command,
            json.dumps(
                _live_source_status(
                    JobState.RUNNING,
                    runtime_metadata=metadata,
                )
            ),
        )

    with pytest.raises(RelayError, match="unsupported schema version"):
        _wait_for_live_structured_runtime_metadata(
            ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            _LIVE_SOURCE_JOB_ID,
            line_prefix="acceptance",
            lines=[],
            timeout_seconds=5,
            poll_seconds=0.01,
            runner=fake_runner,
        )


def test_secure_runtime_orchestration_never_waits_for_outer_job_terminal(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Secure acceptance probes a running service without batch completion checks."""
    pipeline = tmp_path / "secure-runtime.yaml"
    pipeline.write_text("name: secure-runtime\npkgs: []\n", encoding="utf-8")
    commands: list[str] = []
    verifier_calls: list[dict[str, Any]] = []
    config = SecureRuntimeProbeConfig(
        package_name="builtin.paraview",
        command={"command_id": "view-command-41"},
        protocol_adapter=SecureRuntimeProtocolAdapter.model_validate(
            {
                "command_request_id_pointer": "/command_id",
                "health": {
                    "service_instance_id_pointer": "/service_instance_id",
                    "revision_pointer": "/revision",
                },
                "state": {
                    "service_instance_id_pointer": "/service_instance_id",
                    "execution_id_pointer": "/execution_id",
                    "dataset_descriptor_pointer": "/dataset/descriptor",
                    "revision_pointer": "/revision",
                },
                "command": {
                    "service_instance_id_pointer": "/state/service_instance_id",
                    "execution_id_pointer": "/state/execution_id",
                    "dataset_descriptor_pointer": "/state/dataset/descriptor",
                    "revision_pointer": "/state/revision",
                    "command_id_pointer": "/command_id",
                },
                "events": {
                    "service_instance_id_pointer": "/service_instance_id",
                    "execution_id_pointer": "/execution_id",
                    "dataset_descriptor_pointer": "/dataset/descriptor",
                    "revision_pointer": "/revision",
                    "event_name": "state",
                },
            }
        ),
    )

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        script = command[-1]
        commands.append(script)
        if "mkdir -p" in script or "cat >" in script:
            return _completed(command, "")
        if "worker status --cluster test-cluster" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "configured_workload_concurrency": 2,
                        "configured_control_query_concurrency": 1,
                        "control_query_concurrency_consistent": True,
                        "active_leases_by_mcp_admission_class": {
                            "workload": 0,
                            "control_query": 0,
                        },
                        "scan_truncated": False,
                    }
                ),
            )
        if "job submit" in script:
            return _completed(command, f"{_LIVE_SOURCE_JOB_ID}\n")
        if f"job status {_LIVE_SOURCE_JOB_ID}" in script:
            return _completed(
                command,
                json.dumps(
                    _live_source_status(
                        JobState.RUNNING,
                        runtime_metadata=_structured_live_runtime_metadata(),
                    )
                ),
            )
        raise AssertionError(f"unexpected command: {command}")

    def forbidden_batch_call(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("secure runtime acceptance used a batch terminal verifier")

    def verify_secure_runtime(
        _options: LiveAcceptanceOptions,
        *,
        config: SecureRuntimeProbeConfig,
        runtime_metadata: dict[str, Any],
        recorder: ValidationRecorder,
    ) -> set[str]:
        verifier_calls.append(
            {
                "config": config,
                "runtime_metadata": runtime_metadata,
                "recorder": recorder,
            }
        )
        return set()

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def configured_probe(_pipeline_yaml: str) -> SecureRuntimeProbeConfig:
        return config

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_cluster_doctor",
        fake_cluster_doctor,
    )
    monkeypatch.setattr(
        "clio_relay.live_acceptance._secure_runtime_probe_config",
        configured_probe,
    )
    monkeypatch.setattr("clio_relay.live_acceptance._wait_for_success", forbidden_batch_call)
    monkeypatch.setattr("clio_relay.live_acceptance._verify_completed_job", forbidden_batch_call)
    monkeypatch.setattr(
        "clio_relay.live_acceptance._verify_secure_runtime_acceptance",
        verify_secure_runtime,
    )

    report_path = tmp_path / "secure-runtime-report.json"
    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            jarvis_yaml=pipeline,
            report_path=report_path,
            timeout_seconds=5,
            poll_seconds=0.01,
        ),
        runner=fake_runner,
    )

    assert len(verifier_calls) == 1
    assert verifier_calls[0]["runtime_metadata"]["execution_id"] == "execution-live-41"
    assert not any("job wait" in command for command in commands)
    assert not any("job monitor" in command for command in commands)
    assert "acceptance.job_state=running" in lines
    assert "secure-runtime.acceptance=ok" in lines
    assert "live acceptance passed" in lines
    report = load_validation_report(report_path)
    source_check = next(
        check for check in report.checks if check.check_id == "secure-runtime.source-live-metadata"
    )
    assert source_check.status.value == "passed"
    source = next(
        resource
        for resource in report.resources
        if resource.kind == "relay_job" and resource.resource_id == _LIVE_SOURCE_JOB_ID
    )
    assert source.role == "secure_runtime_source"
    assert source.state == "running"
    assert source.metadata["retained"] is True
    assert source.metadata["cancel_scheduler_job"] is False


def test_secure_runtime_query_pending_report_resumes_exact_execution(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A secure query deadline retains source and execution IDs without resubmission."""
    pipeline = tmp_path / "secure-runtime.yaml"
    pipeline.write_text("name: secure-runtime\npkgs: []\n", encoding="utf-8")
    pending_path = tmp_path / "secure-pending.json"
    passed_path = tmp_path / "secure-passed.json"
    submitted = 0
    remote_calls = 0
    verifier_calls = 0
    config = SecureRuntimeProbeConfig(
        package_name="builtin.paraview",
        command={"command_id": "view-command-41"},
        protocol_adapter=_secure_runtime_protocol_adapter(),
    )

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        nonlocal submitted, remote_calls
        del input
        remote_calls += 1
        script = command[-1]
        if "mkdir -p" in script or "cat >" in script:
            return _completed(command, "")
        if "worker status --cluster test-cluster" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "configured_workload_concurrency": 2,
                        "configured_control_query_concurrency": 1,
                        "control_query_concurrency_consistent": True,
                        "active_leases_by_mcp_admission_class": {
                            "workload": 0,
                            "control_query": 0,
                        },
                        "scan_truncated": False,
                    }
                ),
            )
        if "job submit" in script:
            submitted += 1
            return _completed(command, f"{_LIVE_SOURCE_JOB_ID}\n")
        if f"job status {_LIVE_SOURCE_JOB_ID}" in script:
            return _completed(
                command,
                json.dumps(
                    _live_source_status(
                        JobState.RUNNING,
                        runtime_metadata=_structured_live_runtime_metadata(),
                    )
                ),
            )
        raise AssertionError(f"unexpected command: {command}")

    def verify_secure_runtime(
        _options: LiveAcceptanceOptions,
        *,
        config: SecureRuntimeProbeConfig,
        runtime_metadata: dict[str, Any],
        recorder: ValidationRecorder,
    ) -> set[str]:
        nonlocal verifier_calls
        del config, recorder
        verifier_calls += 1
        assert runtime_metadata["pipeline_id"] == "pipeline-live-41"
        assert runtime_metadata["execution_id"] == "execution-live-41"
        if verifier_calls == 1:
            raise _AcceptanceObservationPending(
                "timed out waiting for one ready JARVIS service runtime binding",
                phase="secure_runtime_query",
                identifiers={
                    "pipeline_id": "pipeline-live-41",
                    "execution_id": "execution-live-41",
                },
            )
        return set()

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def configured_probe(_pipeline_yaml: str) -> SecureRuntimeProbeConfig:
        return config

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_cluster_doctor",
        fake_cluster_doctor,
    )
    monkeypatch.setattr(
        "clio_relay.live_acceptance._secure_runtime_probe_config",
        configured_probe,
    )
    monkeypatch.setattr(
        "clio_relay.live_acceptance._verify_secure_runtime_acceptance",
        verify_secure_runtime,
    )
    definition = ClusterDefinition(name="test-cluster", ssh_host="test-host")

    def acceptance_options(
        report_path: Path,
        *,
        resume_report_path: Path | None = None,
    ) -> LiveAcceptanceOptions:
        return LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=definition,
            jarvis_yaml=pipeline,
            timeout_seconds=1,
            poll_seconds=0.01,
            report_path=report_path,
            resume_report_path=resume_report_path,
        )

    run_live_acceptance(
        acceptance_options(pending_path),
        runner=fake_runner,
    )
    pending = load_validation_report(pending_path)
    checkpoint = next(
        resource.metadata["checkpoint"]
        for resource in pending.resources
        if resource.kind == "live_acceptance_checkpoint"
    )
    assert pending.status is ValidationStatus.PENDING
    assert checkpoint["phase"] == "secure_runtime_query"
    assert checkpoint["pipeline_id"] == "pipeline-live-41"
    assert checkpoint["execution_id"] == "execution-live-41"
    assert checkpoint["primary_job_id"] == _LIVE_SOURCE_JOB_ID
    first_remote_calls = remote_calls

    run_live_acceptance(
        acceptance_options(passed_path, resume_report_path=pending_path),
        runner=fake_runner,
    )
    passed = load_validation_report(passed_path)
    assert passed.status is ValidationStatus.PASSED
    assert submitted == 1
    assert verifier_calls == 2
    assert remote_calls == first_remote_calls


def test_secure_runtime_capacity_failure_records_pre_submission_evidence() -> None:
    """A missing reserved lane is diagnosable and cannot create scheduler work."""
    evidence: list[Any] = []

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        return _completed(
            command,
            json.dumps(
                {
                    "configured_workload_concurrency": 1,
                    "configured_control_query_concurrency": 0,
                    "control_query_concurrency_consistent": False,
                    "active_leases_by_mcp_admission_class": {
                        "workload": 0,
                        "control_query": 0,
                    },
                    "worker_generation_id": "endpoint_replacement",
                    "worker_generation_complete": False,
                    "scan_truncated": False,
                }
            ),
        )

    with pytest.raises(RelayError, match="control-query policy is inconsistent"):
        _require_secure_runtime_control_capacity(
            ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            cluster="test-cluster",
            runner=fake_runner,
            evidence=evidence,
        )

    assert len(evidence) == 1
    metadata = evidence[0].metadata
    assert metadata["worker_generation_id"] == "endpoint_replacement"
    assert metadata["worker_generation_complete"] is False
    assert metadata["source_submitted"] is False
    assert metadata["scheduler_job_created"] is False


def test_live_acceptance_stages_files_and_strips_relay_extension(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    input_script = tmp_path / "input.dat"
    input_script.write_text("site input\n", encoding="utf-8")
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        "name: external\n"
        "x_clio_relay:\n"
        "  stage_files:\n"
        "  - local_path: input.dat\n"
        "    remote_path: .local/share/clio-relay/live-tests/{run_id}/input.dat\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  input: .local/share/clio-relay/live-tests/{run_id}/input.dat\n"
        "  progress:\n"
        "    adapter: site-progress\n",
        encoding="utf-8",
    )
    uploaded: list[tuple[str, bytes | None]] = []

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    monitor_calls = 0

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        nonlocal monitor_calls
        script = command[-1]
        if "cat >" in script:
            uploaded.append((script, input))
            return _completed(command, "")
        if "mkdir -p" in script:
            return _completed(command, "")
        if "site_simulation.py" in script:
            return _completed(command, "/opt/site/plugins/site_simulation.py\n")
        if "job submit" in script:
            return _completed(command, "job_abc\n")
        if "job wait" in script:
            return _completed(command, json.dumps({"job_id": "job_abc", "state": "succeeded"}))
        if "job monitor" in script:
            monitor_calls += 1
            events = [
                {"event_type": "job.queued"},
                {"event_type": "job.running"},
                {"event_type": "jarvis.started"},
            ]
            if monitor_calls > 1:
                events.append({"event_type": "job.succeeded"})
            return _completed(
                command,
                json.dumps({"events": events}),
            )
        if "job tasks" in script:
            return _completed(command, json.dumps([{"state": "succeeded"}]))
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"next_offset": 0}))
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                        {"artifact_id": "artifact_provenance", "kind": "provenance"},
                    ]
                ),
            )
        if "read-artifact" in script:
            return _completed(command, json.dumps({"encoding": "base64", "data": "aGVsbG8="}))
        if "job progress" in script:
            return _completed(
                command,
                json.dumps([_provider_progress_record(job_id="job_abc")]),
            )
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)

    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            jarvis_yaml=pipeline,
            report_path=tmp_path / "live-report.json",
        ),
        runner=fake_runner,
    )

    assert "acceptance.application_boundary=package_progress_provider" in lines
    assert "acceptance.package_adapter=site-progress" in lines
    assert "acceptance.package_owner=site.simulation" in lines
    assert "package-progress.provider=verified" in lines
    assert "package-progress.acceptance=verified" in lines
    report = load_validation_report(tmp_path / "live-report.json")
    assert {check.check_id for check in report.checks}.issuperset(
        {"package-progress.provider", "package-progress.acceptance"}
    )
    provider_resource = next(
        resource for resource in report.resources if resource.kind == "package_progress_provider"
    )
    assert provider_resource.state == "verified"
    assert provider_resource.metadata["provider_validated"] is True
    assert provider_resource.metadata["acceptance_validated"] is True
    assert any(item[1] is not None and b"site input" in item[1] for item in uploaded)
    pipeline_upload = uploaded[-1][1]
    assert pipeline_upload is not None
    assert b"x_clio_relay" not in pipeline_upload
    assert b"pkg_type: site.simulation" in pipeline_upload
    assert b"{run_id}" not in pipeline_upload


def test_live_acceptance_requires_worker_provider_attestation_without_local_plugin() -> None:
    pipeline_yaml = (
        "name: external\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  progress:\n"
        "    adapter: site-progress\n"
    )

    assert _expected_progress_adapter(pipeline_yaml) == "site-progress"
    with pytest.raises(RelayError, match="expected package progress adapter"):
        _assert_progress_adapter(
            [
                {
                    "current": 1.0,
                    "metadata": {
                        "adapter": "site-progress",
                        "source": "external",
                        "package_name": "site.simulation",
                    },
                }
            ],
            "site-progress",
            job_id="job_test",
        )
    with pytest.raises(RelayError, match="expected package progress adapter"):
        _assert_progress_adapter(
            [
                {
                    "current": 1.0,
                    "metadata": {
                        "adapter": "site-progress",
                        "source": "jarvis_package",
                        "package_name": "site.simulation",
                        "package_version": "test-plugin",
                        "run_id": "job_test",
                        "execution_id": "job_test",
                    },
                }
            ],
            "site-progress",
            job_id="job_test",
        )
    _assert_progress_adapter(
        [_provider_progress_record()],
        "site-progress",
        job_id="job_test",
    )


def test_live_acceptance_selects_explicit_progress_owner_from_multiple_packages() -> None:
    mixed = (
        "name: mixed\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  progress:\n"
        "    adapter: site-progress\n"
        "- pkg_type: clio_relay.bounded_command\n"
    )

    assert _expected_progress_adapter(mixed) == "site-progress"
    assert _expected_progress_package(mixed) == "site.simulation"


def test_live_acceptance_disables_implicit_multi_package_progress_discovery() -> None:
    mixed = (
        "name: implicit-mixed\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "- pkg_type: clio_relay.bounded_command\n"
    )

    assert _expected_progress_adapter(mixed) is None
    assert _expected_progress_package(mixed) is None


def test_live_acceptance_rejects_multiple_explicit_progress_owners() -> None:
    ambiguous = (
        "name: ambiguous-mixed\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  progress:\n"
        "    adapter: site-progress\n"
        "- pkg_type: another.simulation\n"
        "  progress:\n"
        "    adapter: another-progress\n"
    )

    with pytest.raises(ConfigurationError, match="multiple pipeline packages declare progress"):
        _expected_progress_adapter(ambiguous)
    with pytest.raises(ConfigurationError, match="multiple pipeline packages declare progress"):
        _expected_progress_package(ambiguous)


@pytest.mark.parametrize("adapter", ["''", "1"])
def test_live_acceptance_rejects_invalid_explicit_progress_adapter(adapter: str) -> None:
    invalid = (
        "name: invalid-progress\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  progress:\n"
        f"    adapter: {adapter}\n"
    )

    with pytest.raises(ConfigurationError, match="progress.adapter must be a non-empty string"):
        _expected_progress_adapter(invalid)


def test_live_acceptance_accepts_durable_progress_after_terminal_observation() -> None:

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        script = command[-1]
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ]
                    }
                ),
            )
        if "job progress" in script:
            return _completed(
                command,
                json.dumps([_provider_progress_record()]),
            )
        raise AssertionError(f"unexpected command: {command}")

    _verify_live_package_progress(
        ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        "job_test",
        "site-progress",
        package_name="site.simulation",
        timeout_seconds=1,
        poll_seconds=0.01,
        runner=fake_runner,
    )


def test_live_acceptance_rejects_package_progress_before_running_event() -> None:

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        script = command[-1]
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps({"events": [{"event_type": "job.queued"}]}),
            )
        if "job progress" in script:
            return _completed(
                command,
                json.dumps([_provider_progress_record()]),
            )
        raise AssertionError(f"unexpected command: {command}")

    with pytest.raises(RelayError, match="before job.running"):
        _verify_live_package_progress(
            ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            "job_test",
            "site-progress",
            package_name="site.simulation",
            timeout_seconds=1,
            poll_seconds=0.01,
            runner=fake_runner,
        )


def test_progress_adapter_acceptance_skips_unvalidated_durable_records() -> None:
    progress = [
        _provider_progress_record(
            acceptance_validated=False,
            prediction_status="initializing",
        ),
        _provider_progress_record(),
    ]

    _assert_progress_adapter(
        progress,
        "site-progress",
        job_id="job_test",
        package_name="site.simulation",
    )


def test_live_acceptance_verifies_transport_when_enabled(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    transport_calls: list[dict[str, object]] = []

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_transport(**kwargs: object) -> list[str]:
        transport_calls.append(kwargs)
        return ["transport.healthz=ok", "transport.cleanup=passed"]

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        script = command[-1]
        if "mkdir -p" in script or "cat >" in " ".join(command):
            return _completed(command, "")
        if "job submit" in script:
            return _completed(command, "job_abc\n")
        if "job wait" in script:
            return _completed(command, json.dumps({"job_id": "job_abc", "state": "succeeded"}))
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ]
                    }
                ),
            )
        if "job tasks" in script:
            return _completed(command, json.dumps([{"state": "succeeded"}]))
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"next_offset": 0}))
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                        {"artifact_id": "artifact_provenance", "kind": "provenance"},
                    ]
                ),
            )
        if "read-artifact" in script:
            return _completed(command, json.dumps({"encoding": "base64", "data": "aGVsbG8="}))
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)
    monkeypatch.setattr("clio_relay.live_acceptance.run_frp_http_probe", fake_transport)

    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            jarvis_yaml=pipeline,
            verify_transport=True,
            transport_token="frp-token",
            transport_secret_key="stcp-secret",
            transport_frpc_bin="frpc",
            transport_local_bind_port=19876,
            transport_remote_api_port=8766,
            transport_proxy_name="transport-test",
            api_token="api-token",
        ),
        runner=fake_runner,
    )

    assert "transport.healthz=ok" in lines
    assert transport_calls[0]["token"] == "frp-token"
    assert transport_calls[0]["secret_key"] == "stcp-secret"
    assert transport_calls[0]["local_bind_port"] == 19876
    assert transport_calls[0]["remote_api_port"] == 8766
    assert transport_calls[0]["proxy_name"] == "transport-test"
    assert transport_calls[0]["api_token"] == "api-token"


def test_live_acceptance_report_records_exact_transport_cleanup_resources(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    report_path = tmp_path / "transport-report.json"

    def fake_transport(**_kwargs: object) -> list[str]:
        return [
            "transport.healthz=ok",
            transport_probe_evidence_line(
                TransportProbeEvidence(
                    probe_id="frp-probe-success",
                    cluster="test-cluster",
                    cleanup_mode="transport_probe_teardown",
                    resources=[
                        TransportCleanupResourceEvidence(
                            kind="relay_session",
                            resource_id="frp-probe:success",
                            role="remote_transport_probe_session",
                            location="test-host",
                            action="stop",
                            ownership_verified=True,
                            outcome="stopped",
                            verified_after_operation=True,
                            observed_state="stopped",
                            residual=False,
                            detail=None,
                        ),
                        TransportCleanupResourceEvidence(
                            kind="connector",
                            resource_id="9124",
                            role="remote_frpc_connector",
                            location="test-host",
                            action="stop",
                            ownership_verified=True,
                            outcome="stopped",
                            verified_after_operation=True,
                            observed_state="stopped",
                            residual=False,
                            detail=None,
                            metadata={"pid": 9124},
                        ),
                        TransportCleanupResourceEvidence(
                            kind="gateway_session",
                            resource_id="gateway-live-4",
                            role="gateway_record:close",
                            location="test-host",
                            action="close",
                            ownership_verified=True,
                            outcome="closed",
                            verified_after_operation=True,
                            observed_state="closed",
                            residual=False,
                            detail="owned gateway record closed",
                        ),
                    ],
                )
            ),
            "transport.cleanup=passed",
        ]

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_cluster_doctor",
        fake_cluster_doctor,
    )
    monkeypatch.setattr("clio_relay.live_acceptance.run_frp_http_probe", fake_transport)

    run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            jarvis_yaml=pipeline,
            verify_transport=True,
            transport_token="frp-token",
            transport_secret_key="stcp-secret",
            report_path=report_path,
        ),
        runner=_generic_success_runner(),
    )

    report = load_validation_report(report_path)
    assert report.status.value == "passed"
    assert {(item.kind, item.resource_id) for item in report.resources}.issuperset(
        {
            ("relay_session", "frp-probe:success"),
            ("connector", "9124"),
            ("gateway_session", "gateway-live-4"),
        }
    )
    connector_action = next(
        action for action in report.cleanup.actions if action["resource_id"] == "9124"
    )
    assert connector_action["ownership_verified"] is True
    assert connector_action["observed_state"] == "stopped"
    assert connector_action["residual"] is False
    assert report.cleanup.remaining_resources == []
    assert not any(action.get("kind") == "transport_probe" for action in report.cleanup.actions)


def test_live_acceptance_report_preserves_partial_transport_cleanup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    report_path = tmp_path / "transport-partial-report.json"

    def fake_transport(**_kwargs: object) -> list[str]:
        return [
            "transport.healthz=ok",
            transport_probe_evidence_line(
                TransportProbeEvidence(
                    probe_id="frp-probe-partial",
                    cluster="test-cluster",
                    cleanup_mode="transport_probe_teardown",
                    resources=[
                        TransportCleanupResourceEvidence(
                            kind="connector",
                            resource_id="remote-connector-733",
                            role="remote_frpc_connector",
                            location="test-host",
                            action="stop",
                            ownership_verified=True,
                            outcome="failed",
                            verified_after_operation=False,
                            observed_state="running",
                            residual=True,
                            detail="connector remained after bounded cleanup",
                            metadata={"pid": 733},
                        )
                    ],
                )
            ),
            "transport.cleanup=passed",
        ]

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_cluster_doctor",
        fake_cluster_doctor,
    )
    monkeypatch.setattr("clio_relay.live_acceptance.run_frp_http_probe", fake_transport)

    with pytest.raises(RelayError, match="structured residual resources"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
                jarvis_yaml=pipeline,
                verify_transport=True,
                transport_token="frp-token",
                transport_secret_key="stcp-secret",
                report_path=report_path,
            ),
            runner=_generic_success_runner(),
        )

    report = load_validation_report(report_path)
    assert report.status.value == "failed"
    assert [(item.kind, item.resource_id) for item in report.cleanup.remaining_resources] == [
        ("connector", "remote-connector-733")
    ]
    remaining = report.cleanup.remaining_resources[0]
    assert remaining.metadata["ownership_verified"] is True
    assert remaining.metadata["observed_state"] == "running"
    assert remaining.metadata["detail"] == "connector remained after bounded cleanup"
    assert report.cleanup.actions[0]["outcome"] == "failed"


def test_live_acceptance_report_ingests_cleanup_evidence_attached_to_probe_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    report_path = tmp_path / "transport-exception-report.json"
    evidence_line = transport_probe_evidence_line(
        TransportProbeEvidence(
            probe_id="frp-probe-exception",
            cluster="test-cluster",
            cleanup_mode="transport_probe_teardown",
            resources=[
                TransportCleanupResourceEvidence(
                    kind="relay_session",
                    resource_id="frp-probe:exception",
                    role="remote_transport_probe_session",
                    location="test-host",
                    action="stop",
                    ownership_verified=False,
                    outcome="unknown",
                    verified_after_operation=False,
                    observed_state="running_or_unknown",
                    residual=True,
                    detail="cleanup command returned malformed evidence",
                )
            ],
        )
    )

    def fake_transport(**_kwargs: object) -> list[str]:
        error = RelayError("transport probe failed during cleanup")
        error.__dict__["_clio_relay_transport_evidence_lines"] = [evidence_line]
        raise error

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_cluster_doctor",
        fake_cluster_doctor,
    )
    monkeypatch.setattr("clio_relay.live_acceptance.run_frp_http_probe", fake_transport)

    with pytest.raises(RelayError, match="failed during cleanup"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
                jarvis_yaml=pipeline,
                verify_transport=True,
                transport_token="frp-token",
                transport_secret_key="stcp-secret",
                report_path=report_path,
            ),
            runner=_generic_success_runner(),
        )

    report = load_validation_report(report_path)
    assert report.status.value == "failed"
    assert report.cleanup.actions[0]["resource_id"] == "frp-probe:exception"
    assert report.cleanup.remaining_resources[0].resource_id == "frp-probe:exception"


def test_live_acceptance_rejects_transport_without_verified_cleanup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_transport(**_kwargs: object) -> list[str]:
        return ["transport.healthz=ok"]

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_cluster_doctor",
        fake_cluster_doctor,
    )
    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_frp_http_probe",
        fake_transport,
    )

    with pytest.raises(RelayError, match="transport cleanup evidence is incomplete"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
                jarvis_yaml=pipeline,
                verify_transport=True,
                transport_token="frp-token",
                transport_secret_key="stcp-secret",
            ),
            runner=_generic_success_runner(),
        )


def test_live_acceptance_transport_requires_secrets(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="frp token"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
                jarvis_yaml=pipeline,
                verify_transport=True,
            )
        )


def test_live_acceptance_verifies_direct_transport_when_enabled(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    transport_calls: list[dict[str, object]] = []

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_direct_transport(**kwargs: object) -> list[str]:
        transport_calls.append(kwargs)
        return [
            "direct_transport.result=xtcp",
            "transport.proxy_type=xtcp",
            "transport.healthz=ok",
            "transport.http_wait=succeeded",
            "transport.cleanup=passed",
        ]

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)
    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_frp_direct_http_probe",
        fake_direct_transport,
    )

    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            jarvis_yaml=pipeline,
            verify_direct_transport=True,
            transport_token="frp-token",
            transport_secret_key="xtcp-secret",
            transport_frpc_bin="frpc",
            transport_local_bind_port=19876,
            transport_remote_api_port=8766,
            transport_proxy_name="direct-test",
            api_token="api-token",
        ),
        runner=_generic_success_runner(),
    )

    assert "direct_transport.result=xtcp" in lines
    assert "transport.proxy_type=xtcp" in lines
    assert transport_calls[0]["token"] == "frp-token"
    assert transport_calls[0]["secret_key"] == "xtcp-secret"
    assert transport_calls[0]["allow_stcp_fallback"] is False
    assert transport_calls[0]["local_bind_port"] == 19876
    assert transport_calls[0]["remote_api_port"] == 8766
    assert transport_calls[0]["proxy_name"] == "direct-test"
    assert transport_calls[0]["api_token"] == "api-token"


def test_live_acceptance_verifies_configured_direct_transport(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    transport_calls: list[dict[str, object]] = []

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_direct_transport(**kwargs: object) -> list[str]:
        transport_calls.append(kwargs)
        return [
            "direct_transport.result=xtcp",
            "transport.proxy_type=xtcp",
            "transport.healthz=ok",
            "transport.http_wait=succeeded",
            "transport.cleanup=passed",
        ]

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)
    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_frp_direct_http_probe",
        fake_direct_transport,
    )

    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(
                name="test-cluster",
                ssh_host="test-host",
                live_test=LiveTestConfig(
                    verify_direct_transport=True,
                    allow_direct_transport_fallback=False,
                ),
            ),
            jarvis_yaml=pipeline,
            transport_token="frp-token",
            transport_secret_key="xtcp-secret",
        ),
        runner=_generic_success_runner(),
    )

    assert "direct_transport.result=xtcp" in lines
    assert transport_calls[0]["allow_stcp_fallback"] is False


def test_live_acceptance_rejects_direct_transport_fallback_unless_allowed(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_direct_transport(**_kwargs: object) -> list[str]:
        return [
            "direct_transport.result=frp_stcp",
            "transport.proxy_type=stcp",
            "transport.healthz=ok",
        ]

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)
    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_frp_direct_http_probe",
        fake_direct_transport,
    )

    with pytest.raises(RelayError, match="did not prove XTCP"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
                jarvis_yaml=pipeline,
                verify_direct_transport=True,
                transport_token="frp-token",
                transport_secret_key="xtcp-secret",
            ),
            runner=_generic_success_runner(),
        )


def test_live_acceptance_requires_full_direct_xtcp_evidence(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_direct_transport(**_kwargs: object) -> list[str]:
        return [
            "direct_transport.result=xtcp",
            "transport.healthz=ok",
        ]

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)
    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_frp_direct_http_probe",
        fake_direct_transport,
    )

    with pytest.raises(RelayError, match="transport.http_wait=succeeded"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
                jarvis_yaml=pipeline,
                verify_direct_transport=True,
                transport_token="frp-token",
                transport_secret_key="xtcp-secret",
            ),
            runner=_generic_success_runner(),
        )


def test_live_acceptance_runs_configured_pipeline_and_monitor(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    commands: list[list[str]] = []
    uploaded: list[bytes | None] = []

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        uploaded.append(input)
        if "cat >" in " ".join(command):
            return _completed(command, "")
        script = command[-1]
        if "mkdir -p" in script:
            return _completed(command, "")
        if "job submit" in script:
            return _completed(command, "job_abc\n")
        if "job wait" in script:
            return _completed(command, json.dumps({"job_id": "job_abc", "state": "succeeded"}))
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ]
                    }
                ),
            )
        if "job tasks" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {
                            "task_id": "task_abc",
                            "name": "jarvis.execution",
                            "state": "succeeded",
                        }
                    ]
                ),
            )
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"next_offset": 0}))
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                        {"artifact_id": "artifact_provenance", "kind": "provenance"},
                    ]
                ),
            )
        if "read-artifact" in script:
            return _completed(command, json.dumps({"encoding": "base64", "data": "aGVsbG8="}))
        if "monitor add-regex" in script:
            return _completed(command, json.dumps({"rule_id": "rule_abc"}))
        if "monitor run-once" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"action": "emit_event"},
                        {"action": "record_progress", "progress_id": "progress_abc"},
                    ]
                ),
            )
        if "job progress" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {
                            "progress_id": "progress_abc",
                            "label": "iteration",
                            "current": 5,
                            "total": 10,
                        }
                    ]
                ),
            )
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)

    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(
                name="test-cluster",
                ssh_host="test-host",
                live_test=LiveTestConfig(monitor_pattern="done"),
            ),
            jarvis_yaml=pipeline,
            progress_pattern=r"step=(?P<step>\d+)",
            progress_action_payload={
                "label": "iteration",
                "current_group": "step",
                "total": 10,
                "unit": "step",
            },
        ),
        runner=fake_runner,
    )

    assert "acceptance.job_state=succeeded" in lines
    assert "acceptance.tasks=1" in lines
    assert "acceptance.artifact_read=ok" in lines
    assert "acceptance.provenance=ok" in lines
    assert "acceptance.monitor=ok" in lines
    assert "acceptance.progress=1" in lines
    assert "live acceptance passed" in lines
    assert any(item is not None and b"name: generic" in item for item in uploaded)
    assert any("job submit" in " ".join(command) for command in commands)
    assert any(
        'CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis"' in command[-1] for command in commands
    )


def test_live_acceptance_uses_cluster_executable_overrides(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    commands: list[list[str]] = []

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        script = command[-1]
        if "mkdir -p" in script or "cat >" in " ".join(command):
            return _completed(command, "")
        if "job submit" in script:
            return _completed(command, "job_abc\n")
        if "job wait" in script:
            return _completed(command, json.dumps({"job_id": "job_abc", "state": "succeeded"}))
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ]
                    }
                ),
            )
        if "job tasks" in script:
            return _completed(
                command,
                json.dumps([{"task_id": "task_abc", "state": "succeeded"}]),
            )
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"next_offset": 0}))
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                        {"artifact_id": "artifact_provenance", "kind": "provenance"},
                    ]
                ),
            )
        if "read-artifact" in script:
            return _completed(command, json.dumps({"encoding": "base64", "data": "aGVsbG8="}))
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)

    run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(
                name="test-cluster",
                ssh_host="test-host",
                jarvis_bin="/opt/jarvis/current",
                frpc_bin="/opt/frp/frpc",
                agent_bin="/opt/agents/clio",
            ),
            jarvis_yaml=pipeline,
        ),
        runner=fake_runner,
    )

    rendered = "\n".join(command[-1] for command in commands)
    assert 'CLIO_RELAY_JARVIS_BIN="/opt/jarvis/current"' in rendered
    assert 'CLIO_RELAY_FRPC_BIN="/opt/frp/frpc"' in rendered
    assert 'CLIO_RELAY_AGENT_BIN="/opt/agents/clio"' in rendered


def test_live_acceptance_uses_fresh_idempotency_key_per_run(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    submitted_scripts: list[str] = []

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        script = command[-1]
        if "job submit" in script:
            submitted_scripts.append(script)
            return _completed(command, f"job_{len(submitted_scripts)}\n")
        if "job wait" in script:
            job_id = f"job_{len(submitted_scripts)}"
            return _completed(command, json.dumps({"job_id": job_id, "state": "succeeded"}))
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ]
                    }
                ),
            )
        if "job tasks" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {
                            "task_id": "task_abc",
                            "name": "jarvis.execution",
                            "state": "succeeded",
                        }
                    ]
                ),
            )
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"next_offset": 0}))
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                        {"artifact_id": "artifact_provenance", "kind": "provenance"},
                    ]
                ),
            )
        if "read-artifact" in script:
            return _completed(command, json.dumps({"encoding": "base64", "data": "aGVsbG8="}))
        return _completed(command, "")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)

    for _ in range(2):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
                jarvis_yaml=pipeline,
            ),
            runner=fake_runner,
        )

    assert len(submitted_scripts) == 2
    assert submitted_scripts[0] != submitted_scripts[1]
    assert "live-test:test-cluster:" in submitted_scripts[0]
    assert "live-test:test-cluster:" in submitted_scripts[1]


def test_live_acceptance_pending_wait_resumes_exact_job_without_resubmission(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Repeated bounded observations retain one exact HPC workload indefinitely."""
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    first_report_path = tmp_path / "pending.json"
    second_report_path = tmp_path / "pending-again.json"
    final_report_path = tmp_path / "passed.json"
    job_id = "job_11111111111111111111111111111111"
    submitted = 0

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_cluster_doctor",
        fake_cluster_doctor,
    )

    def runner_for_state(state: str) -> CommandRunner:
        def fake_runner(
            command: list[str],
            *,
            input: bytes | None = None,
        ) -> subprocess.CompletedProcess[bytes]:
            nonlocal submitted
            del input
            script = command[-1]
            if "mkdir -p" in script or "cat >" in " ".join(command):
                return _completed(command, "")
            if "job submit" in script:
                submitted += 1
                return _completed(command, f"{job_id}\n")
            if "job wait" in script:
                return _completed(command, json.dumps({"job_id": job_id, "state": state}))
            if "job monitor" in script:
                return _completed(
                    command,
                    json.dumps(
                        {
                            "events": [
                                {"event_type": "job.queued"},
                                {"event_type": "job.running"},
                                {"event_type": "jarvis.started"},
                                {"event_type": "job.succeeded"},
                            ]
                        }
                    ),
                )
            if "job tasks" in script:
                return _completed(
                    command,
                    json.dumps([{"task_id": "task_abc", "state": "succeeded"}]),
                )
            if "read-log" in script and "--stream stdout" in script:
                return _completed(command, json.dumps({"next_offset": 12}))
            if "read-log" in script and "--stream stderr" in script:
                return _completed(command, json.dumps({"next_offset": 0}))
            if "list-artifacts" in script:
                return _completed(
                    command,
                    json.dumps(
                        [
                            {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                            {"artifact_id": "artifact_stdout", "kind": "stdout"},
                            {"artifact_id": "artifact_stderr", "kind": "stderr"},
                            {"artifact_id": "artifact_provenance", "kind": "provenance"},
                        ]
                    ),
                )
            if "read-artifact" in script:
                return _completed(
                    command,
                    json.dumps({"encoding": "base64", "data": "aGVsbG8="}),
                )
            raise AssertionError(f"unexpected command: {command}")

        return fake_runner

    definition = ClusterDefinition(name="test-cluster", ssh_host="test-host")

    def acceptance_options(
        report_path: Path,
        *,
        resume_report_path: Path | None = None,
    ) -> LiveAcceptanceOptions:
        return LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=definition,
            jarvis_yaml=pipeline,
            timeout_seconds=1,
            poll_seconds=0.01,
            report_path=report_path,
            resume_report_path=resume_report_path,
        )

    first_lines = run_live_acceptance(
        acceptance_options(first_report_path),
        runner=runner_for_state("queued"),
    )
    first = load_validation_report(first_report_path)
    assert first.status is ValidationStatus.PENDING
    assert "validation.status=pending" in first_lines
    checkpoint_resource = next(
        resource for resource in first.resources if resource.kind == "live_acceptance_checkpoint"
    )
    first_selector = checkpoint_resource.metadata["retry_selector"]
    assert first_selector["primary_job_id"] == job_id
    assert checkpoint_resource.metadata["checkpoint_has_ttl"] is False
    assert submitted == 1

    second_lines = run_live_acceptance(
        acceptance_options(second_report_path, resume_report_path=first_report_path),
        runner=runner_for_state("running"),
    )
    second = load_validation_report(second_report_path)
    second_checkpoint = next(
        resource for resource in second.resources if resource.kind == "live_acceptance_checkpoint"
    )
    assert second.status is ValidationStatus.PENDING
    assert "validation.status=pending" in second_lines
    assert second_checkpoint.metadata["retry_selector"] == first_selector
    assert submitted == 1

    final_lines = run_live_acceptance(
        acceptance_options(final_report_path, resume_report_path=second_report_path),
        runner=runner_for_state("succeeded"),
    )
    final = load_validation_report(final_report_path)
    assert final.status is ValidationStatus.PASSED
    assert "acceptance.job_state=succeeded" in final_lines
    assert submitted == 1
    assert not any(resource.kind == "live_acceptance_checkpoint" for resource in final.resources)


def test_live_acceptance_rejects_tampered_resume_before_remote_mutation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Checkpoint identity and integrity are verified before SSH or relay submission."""
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    pending_path = tmp_path / "pending.json"
    tampered_path = tmp_path / "tampered.json"
    resumed_path = tmp_path / "resume-failure.json"
    job_id = "job_22222222222222222222222222222222"

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_cluster_doctor",
        fake_cluster_doctor,
    )

    def initial_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        script = command[-1]
        if "mkdir -p" in script or "cat >" in " ".join(command):
            return _completed(command, "")
        if "job submit" in script:
            return _completed(command, f"{job_id}\n")
        if "job wait" in script:
            return _completed(command, json.dumps({"job_id": job_id, "state": "queued"}))
        raise AssertionError(f"unexpected command: {command}")

    options = LiveAcceptanceOptions(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        jarvis_yaml=pipeline,
        report_path=pending_path,
        timeout_seconds=1,
        poll_seconds=0.01,
    )
    run_live_acceptance(options, runner=initial_runner)
    document = json.loads(pending_path.read_text(encoding="utf-8"))
    checkpoint = next(
        resource
        for resource in document["resources"]
        if resource["kind"] == "live_acceptance_checkpoint"
    )
    checkpoint["metadata"]["checkpoint"]["primary_job_id"] = "job_ffffffffffffffffffffffffffffffff"
    tampered_path.write_text(json.dumps(document), encoding="utf-8")
    remote_calls = 0

    def forbidden_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        nonlocal remote_calls
        del input
        remote_calls += 1
        raise AssertionError(f"resume must fail before remote command: {command}")

    with pytest.raises(ConfigurationError, match="checkpoint is invalid"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=options.definition,
                jarvis_yaml=pipeline,
                report_path=resumed_path,
                resume_report_path=tampered_path,
                timeout_seconds=1,
                poll_seconds=0.01,
            ),
            runner=forbidden_runner,
        )
    assert remote_calls == 0


def test_live_acceptance_requires_agent_child_job_when_mcp_configured(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    commands: list[list[str]] = []
    primary_job_id = "job_11111111111111111111111111111111"
    agent_job_id = "job_22222222222222222222222222222222"
    child_job_id = "job_33333333333333333333333333333333"

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        script = command[-1]
        if "mkdir -p" in script or "cat >" in " ".join(command):
            return _completed(command, "")
        if "job submit" in script:
            return _completed(command, f"{primary_job_id}\n")
        if "agent run" in script:
            return _completed(command, f"{agent_job_id}\n")
        if f"job wait {primary_job_id}" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "job_id": primary_job_id,
                        "state": "succeeded",
                        "created_at": "2026-07-07T00:00:00Z",
                    }
                ),
            )
        if f"job wait {agent_job_id}" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "job_id": agent_job_id,
                        "state": "succeeded",
                        "created_at": "2026-07-07T00:01:00Z",
                    }
                ),
            )
        if f"job wait {child_job_id}" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "job_id": child_job_id,
                        "state": "succeeded",
                        "created_at": "2026-07-07T00:02:00Z",
                    }
                ),
            )
        if "job monitor" in script:
            job_id = child_job_id if child_job_id in script else primary_job_id
            created_at = (
                "2026-07-07T00:02:00Z" if child_job_id in script else "2026-07-07T00:00:00Z"
            )
            return _completed(
                command,
                json.dumps(
                    {
                        "job": {
                            "job_id": job_id,
                            "state": "succeeded",
                            "created_at": created_at,
                        },
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ],
                    }
                ),
            )
        if "job tasks" in script:
            return _completed(
                command,
                json.dumps([{"task_id": "task_abc", "state": "succeeded"}]),
            )
        if "read-log" in script and agent_job_id in script:
            return _completed(
                command,
                json.dumps({"text": f"submitted {child_job_id}\n", "next_offset": 37}),
            )
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"text": "ok\n", "next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"text": "", "next_offset": 0}))
        if "list-artifacts" in script and agent_job_id in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_agent_result", "kind": "agent_result"},
                        {"artifact_id": "artifact_agent_message", "kind": "agent_last_message"},
                    ]
                ),
            )
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                        {"artifact_id": "artifact_provenance", "kind": "provenance"},
                    ]
                ),
            )
        if "read-artifact artifact_agent_result" in script:
            return _completed(command, _artifact_json('{"returncode": 0}'))
        if "read-artifact artifact_agent_message" in script:
            return _completed(command, _artifact_json(f"submitted {child_job_id}\n"))
        if "read-artifact" in script:
            return _completed(command, _artifact_json("artifact bytes"))
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)

    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            jarvis_yaml=pipeline,
            agent_prompt="/remote/prompt.md",
            agent_mcp_config="/remote/mcp.toml",
        ),
        runner=fake_runner,
    )

    assert f"acceptance.agent_job_id={agent_job_id}" in lines
    assert f"acceptance.agent_child_job_id={child_job_id}" in lines
    assert "acceptance.agent_child.provenance=ok" in lines
    assert any(f"job wait {child_job_id}" in command[-1] for command in commands)


def test_live_acceptance_generates_agent_prompt_from_child_pipeline(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: primary\npkgs: []\n", encoding="utf-8")
    child_input = tmp_path / "child.in"
    child_input.write_text("run 5\n", encoding="utf-8")
    child_pipeline = tmp_path / "child.yaml"
    child_pipeline.write_text(
        "name: child-workload\n"
        "x_clio_relay:\n"
        "  stage_files:\n"
        "  - local_path: child.in\n"
        "    remote_path: .local/share/clio-relay/live-tests/{run_id}/child.in\n"
        "pkgs:\n"
        "- pkg_type: example.child\n"
        "  script: .local/share/clio-relay/live-tests/{run_id}/child.in\n",
        encoding="utf-8",
    )
    uploads: dict[str, bytes | None] = {}
    commands: list[list[str]] = []
    primary_job_id = "job_11111111111111111111111111111111"
    agent_job_id = "job_22222222222222222222222222222222"
    child_job_id = "job_33333333333333333333333333333333"

    def fake_cluster_doctor(_definition: ClusterDefinition) -> list[str]:
        return ["cluster: test-cluster"]

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        script = command[-1]
        if 'printf "%s" "$HOME"' in script:
            return _completed(command, "/home/test-user")
        if "mkdir -p" in script:
            return _completed(command, "")
        if "cat >" in " ".join(command):
            remote_path = script.split("cat > ", maxsplit=1)[1].split(" &&", maxsplit=1)[0]
            uploads[remote_path.strip("'")] = input
            return _completed(command, "")
        if "job submit" in script:
            return _completed(command, f"{primary_job_id}\n")
        if "agent run" in script:
            return _completed(command, f"{agent_job_id}\n")
        if f"job wait {primary_job_id}" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "job_id": primary_job_id,
                        "state": "succeeded",
                        "created_at": "2026-07-07T00:00:00Z",
                    }
                ),
            )
        if f"job wait {agent_job_id}" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "job_id": agent_job_id,
                        "state": "succeeded",
                        "created_at": "2026-07-07T00:01:00Z",
                    }
                ),
            )
        if f"job wait {child_job_id}" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "job_id": child_job_id,
                        "state": "succeeded",
                        "created_at": "2026-07-07T00:02:00Z",
                    }
                ),
            )
        if "job monitor" in script:
            job_id = child_job_id if child_job_id in script else primary_job_id
            created_at = (
                "2026-07-07T00:02:00Z" if child_job_id in script else "2026-07-07T00:00:00Z"
            )
            return _completed(
                command,
                json.dumps(
                    {
                        "job": {
                            "job_id": job_id,
                            "state": "succeeded",
                            "created_at": created_at,
                        },
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ],
                    }
                ),
            )
        if "job tasks" in script:
            return _completed(
                command,
                json.dumps([{"task_id": "task_abc", "state": "succeeded"}]),
            )
        if "read-log" in script and agent_job_id in script:
            return _completed(
                command,
                json.dumps({"text": f"submitted {child_job_id}\n", "next_offset": 37}),
            )
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"text": "ok\n", "next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"text": "", "next_offset": 0}))
        if "list-artifacts" in script and agent_job_id in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_agent_result", "kind": "agent_result"},
                        {"artifact_id": "artifact_agent_message", "kind": "agent_last_message"},
                    ]
                ),
            )
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                        {"artifact_id": "artifact_provenance", "kind": "provenance"},
                    ]
                ),
            )
        if "read-artifact artifact_agent_result" in script:
            return _completed(command, _artifact_json('{"returncode": 0}'))
        if "read-artifact artifact_agent_message" in script:
            return _completed(command, _artifact_json(f"submitted {child_job_id}\n"))
        if "read-artifact" in script:
            return _completed(command, _artifact_json("artifact bytes"))
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)

    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            jarvis_yaml=pipeline,
            agent_child_jarvis_yaml=child_pipeline,
            agent_mcp_config="/remote/mcp.toml",
        ),
        runner=fake_runner,
    )

    prompt_uploads = {
        path: content for path, content in uploads.items() if path.endswith("/agent-prompt.md")
    }
    assert len(prompt_uploads) == 1
    prompt = next(iter(prompt_uploads.values()))
    assert prompt is not None
    prompt_text = prompt.decode("utf-8")
    assert "cluster: test-cluster" in prompt_text
    assert "name: child-workload" in prompt_text
    assert "x_clio_relay" not in prompt_text
    assert "{run_id}" not in prompt_text
    assert "script: .local/share/clio-relay/live-tests/" in prompt_text
    assert "idempotency_key: live-test:test-cluster:" in prompt_text
    assert any(content is not None and b"run 5" in content for content in uploads.values())
    assert "acceptance.agent_child.provenance=ok" in lines
    assert any(
        "/agent-prompt.md" in command[-1] for command in commands if "agent run" in command[-1]
    )


def test_agent_child_job_must_be_created_by_current_agent_run() -> None:
    agent_job_id = "job_22222222222222222222222222222222"
    stale_child_job_id = "job_33333333333333333333333333333333"

    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        script = command[-1]
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_agent_result", "kind": "agent_result"},
                        {"artifact_id": "artifact_agent_message", "kind": "agent_last_message"},
                    ]
                ),
            )
        if "read-artifact artifact_agent_result" in script:
            return _completed(command, _artifact_json('{"returncode": 0}'))
        if "read-artifact artifact_agent_message" in script:
            return _completed(command, _artifact_json(f"submitted {stale_child_job_id}\n"))
        if "read-log" in script:
            return _completed(command, json.dumps({"text": "", "next_offset": 0}))
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "job": {
                            "job_id": stale_child_job_id,
                            "state": "succeeded",
                            "created_at": "2026-07-07T00:00:00Z",
                        },
                        "events": [],
                    }
                ),
            )
        raise AssertionError(f"unexpected command: {command}")

    with pytest.raises(RelayError, match="stale child"):
        _find_agent_child_job(
            ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            agent_job_id,
            agent_created_at="2026-07-07T00:01:00Z",
            runner=fake_runner,
        )


def _completed(command: list[str], stdout: str) -> subprocess.CompletedProcess[bytes]:
    script = command[-1] if command else ""
    record_key = None
    if "job tasks" in script:
        record_key = "tasks"
    elif "job progress" in script:
        record_key = "progress"
    elif "list-artifacts" in script:
        record_key = "artifacts"
    if record_key is not None:
        decoded = cast(object, json.loads(stdout))
        if isinstance(decoded, list):
            records: list[dict[str, object]] = []
            for item in cast(list[object], decoded):
                if isinstance(item, dict):
                    records.append(
                        {str(key): value for key, value in cast(dict[object, object], item).items()}
                    )
            stdout = json.dumps(_collection_page(record_key, records))
    return subprocess.CompletedProcess(command, 0, stdout=stdout.encode(), stderr=b"")


def _generic_success_runner() -> CommandRunner:
    def fake_runner(
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        script = command[-1]
        if "mkdir -p" in script or "cat >" in " ".join(command):
            return _completed(command, "")
        if "job submit" in script:
            return _completed(command, "job_abc\n")
        if "job wait" in script:
            return _completed(command, json.dumps({"job_id": "job_abc", "state": "succeeded"}))
        if "job monitor" in script:
            return _completed(
                command,
                json.dumps(
                    {
                        "events": [
                            {"event_type": "job.queued"},
                            {"event_type": "job.running"},
                            {"event_type": "jarvis.started"},
                            {"event_type": "job.succeeded"},
                        ]
                    }
                ),
            )
        if "job tasks" in script:
            return _completed(command, json.dumps([{"state": "succeeded"}]))
        if "read-log" in script and "--stream stdout" in script:
            return _completed(command, json.dumps({"next_offset": 12}))
        if "read-log" in script and "--stream stderr" in script:
            return _completed(command, json.dumps({"next_offset": 0}))
        if "list-artifacts" in script:
            return _completed(
                command,
                json.dumps(
                    [
                        {"artifact_id": "artifact_pipeline", "kind": "jarvis_pipeline"},
                        {"artifact_id": "artifact_stdout", "kind": "stdout"},
                        {"artifact_id": "artifact_stderr", "kind": "stderr"},
                        {"artifact_id": "artifact_provenance", "kind": "provenance"},
                    ]
                ),
            )
        if "read-artifact" in script:
            return _completed(command, json.dumps({"encoding": "base64", "data": "aGVsbG8="}))
        raise AssertionError(f"unexpected command: {command}")

    return fake_runner


def _artifact_json(text: str) -> str:
    return json.dumps(
        {
            "encoding": "base64",
            "data": b64encode(text.encode("utf-8")).decode("ascii"),
        }
    )


def test_secure_runtime_probe_config_is_generic_strict_and_bounded() -> None:
    """Operators can select any package runtime without site or application code."""
    configured = _secure_runtime_probe_config(
        """
name: remote-service
x_clio_relay:
  secure_runtime_probe:
    package_name: builtin.paraview
    package_id: paraview-7
    command:
      command_id: view-command-41
      schema_version: jarvis.paraview.command.v2
      operation: set_timestep
      expected_revision: 4
      arguments: {index: 1}
    protocol_adapter:
      command_request_id_pointer: /command_id
      health:
        assertions:
          /schema_version: jarvis.paraview.health.v1
          /status: ready
        service_instance_id_pointer: /service_instance_id
        revision_pointer: /revision
      state:
        assertions: {/schema_version: jarvis.paraview.service-state.v2}
        service_instance_id_pointer: /service_instance_id
        execution_id_pointer: /execution_id
        dataset_descriptor_pointer: /dataset/descriptor
        revision_pointer: /revision
      command:
        assertions:
          /schema_version: jarvis.paraview.command-result.v2
          /applied: true
        service_instance_id_pointer: /state/service_instance_id
        execution_id_pointer: /state/execution_id
        dataset_descriptor_pointer: /state/dataset/descriptor
        revision_pointer: /state/revision
        command_id_pointer: /command_id
      events:
        assertions: {/schema_version: jarvis.paraview.service-state.v2}
        service_instance_id_pointer: /service_instance_id
        execution_id_pointer: /execution_id
        dataset_descriptor_pointer: /dataset/descriptor
        revision_pointer: /revision
        event_name: state
pkgs:
- pkg_type: builtin.paraview
"""
    )

    assert configured == SecureRuntimeProbeConfig(
        package_name="builtin.paraview",
        package_id="paraview-7",
        command={
            "command_id": "view-command-41",
            "schema_version": "jarvis.paraview.command.v2",
            "operation": "set_timestep",
            "expected_revision": 4,
            "arguments": {"index": 1},
        },
        protocol_adapter=_secure_runtime_protocol_adapter(),
    )
    with pytest.raises(ConfigurationError, match="secure_runtime_probe is invalid"):
        _secure_runtime_probe_config(
            """
x_clio_relay:
  secure_runtime_probe:
    package_name: site.custom-visualizer
    command: {command_id: view-command-41, action: set-view}
    protocol_adapter: {}
    cluster: hardcoded-site
"""
        )
    with pytest.raises(ConfigurationError, match="secure_runtime_probe is invalid"):
        _secure_runtime_probe_config(
            """
x_clio_relay:
  secure_runtime_probe:
    package_name: true
    browser_attachment_ttl_seconds: "300"
    command: {command_id: view-command-41, action: set-view}
    protocol_adapter: {}
"""
        )


def test_secure_runtime_secret_scanner_rejects_capabilities_without_key_false_positives() -> None:
    """Secret proof examines values while allowing redacted credential field names."""
    _assert_secret_free_document(
        {
            "api_token": "<redacted>",
            "secrets_absent": True,
            "authorization_sha256": "a" * 64,
        },
        forbidden_values={"transport-token-value"},
        label="redacted report",
    )
    with pytest.raises(RelayError, match="private capability"):
        _assert_secret_free_document(
            {"evidence": "prefix transport-token-value suffix"},
            forbidden_values={"transport-token-value"},
            label="leaking report",
        )
    with pytest.raises(RelayError, match="browser capability URL"):
        _assert_secret_free_document(
            {"url": "http://127.0.0.1:9999/health?capability=abc"},
            forbidden_values=set(),
            label="leaking report",
        )


def test_secure_runtime_browser_http_is_direct_strict_bounded_and_deadlined(
    monkeypatch: MonkeyPatch,
) -> None:
    """Browser probes bypass proxies/redirects and reject slow, duplicate, or flooded data."""
    paths: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            paths.append(self.path)
            path = self.path.partition("?")[0]
            if path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/ok")
                self.end_headers()
                return
            if path == "/flood":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(1024 * 1024 + 1))
                self.end_headers()
                return
            if path == "/slow":
                payload = b'{"status":"ready"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                for byte in payload:
                    try:
                        self.wfile.write(bytes([byte]))
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        return
                    time.sleep(0.04)
                return
            if path == "/events-duplicate":
                payload = b'event: state\ndata: {"revision":1,"revision":2}\n\n'
                content_type = "text/event-stream"
            elif path == "/duplicate":
                payload = b'{"revision":1,"revision":2}'
                content_type = "application/json"
            else:
                payload = b'{"status":"ready","revision":1}'
                content_type = "application/json; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = int(server.server_address[1])
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")
    try:
        observation, document = _browser_json_observation(
            f"http://127.0.0.1:{port}/ok?capability={'a' * 64}",
            endpoint="health",
            method="GET",
            body=None,
            timeout_seconds=1,
        )
        assert observation.status_code == 200
        assert document["status"] == "ready"
        ok_requests = sum(path.startswith("/ok?") for path in paths)

        with pytest.raises(_BrowserHttpRequestError) as redirect:
            _browser_json_observation(
                f"http://127.0.0.1:{port}/redirect?capability={'a' * 64}",
                endpoint="health",
                method="GET",
                body=None,
                timeout_seconds=1,
            )
        assert redirect.value.kind == "http_302"
        assert sum(path.startswith("/ok?") for path in paths) == ok_requests

        with pytest.raises(RelayError, match="strict finite JSON"):
            _browser_json_observation(
                f"http://127.0.0.1:{port}/duplicate?capability={'a' * 64}",
                endpoint="state",
                method="GET",
                body=None,
                timeout_seconds=1,
            )
        with pytest.raises(RelayError, match="strict finite JSON"):
            _browser_sse_observation(
                f"http://127.0.0.1:{port}/events-duplicate?capability={'a' * 64}",
                timeout_seconds=1,
                expected_event_name="state",
            )
        with pytest.raises(_BrowserHttpRequestError) as flood:
            _browser_json_observation(
                f"http://127.0.0.1:{port}/flood?capability={'a' * 64}",
                endpoint="state",
                method="GET",
                body=None,
                timeout_seconds=1,
            )
        assert flood.value.kind == "flood"

        started = time.monotonic()
        with pytest.raises(_BrowserHttpRequestError) as slow:
            _browser_json_observation(
                f"http://127.0.0.1:{port}/slow?capability={'a' * 64}",
                endpoint="health",
                method="GET",
                body=None,
                timeout_seconds=0.12,
            )
        assert slow.value.kind == "deadline"
        assert time.monotonic() - started < 0.75
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_secure_runtime_acceptance_records_exact_v35_browser_and_cleanup_path(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The release probe emits complete secret-free evidence for one arbitrary target."""
    cluster = "operator-edge-west"
    source_job_id = "job_secure_query"
    source_artifact_id = "artifact_secure_query"
    gateway_session_id = "gateway_secure_runtime"
    dataset_descriptor = _secure_runtime_dataset_descriptor()
    dataset_sha256 = _secure_runtime_json_sha256(dataset_descriptor)
    assert dataset_sha256 != cast(dict[str, str], dataset_descriptor["fingerprint"])["digest"]
    handoff = {
        "cluster": cluster,
        "source_job_id": source_job_id,
        "source_artifact_id": source_artifact_id,
        "package_id": "paraview-7",
        "package_name": "builtin.paraview",
        "service_instance_id": "visualizer-service-41",
    }
    source_sha256 = "a" * 64
    binding = {
        "schema_version": "clio-relay.jarvis-service-runtime-binding.v2",
        "source_relay_job_id": source_job_id,
        "source_relay_artifact_id": source_artifact_id,
        "source_relay_artifact_sha256": source_sha256,
        "source_tool": "jarvis_get_execution",
        "jarvis_execution_id": "execution-service-41",
        "scheduler_provider": "slurm",
        "scheduler_native_id": "90141",
        "package_id": "paraview-7",
        "package_name": "builtin.paraview",
        "service_instance_id": "visualizer-service-41",
        "service_revision": 4,
        "service_report_sha256": "b" * 64,
        "service_runtime_schema_version": "jarvis.service-runtime.v2",
        "authorization_sha256": "c" * 64,
        "dataset_descriptor_sha256": dataset_sha256,
        "dataset_descriptor": dataset_descriptor,
    }
    ready_session = GatewaySession(
        session_id=gateway_session_id,
        cluster=cluster,
        name="site-service",
        state=GatewaySessionState.READY,
        scheduler="slurm",
        scheduler_job_id="90141",
        queue_state="running",
        gateway={"jarvis_runtime_binding": binding},
        metadata={"owner": "clio-relay"},
    )
    query_result: dict[str, Any] = {
        "terminal": True,
        "state": "succeeded",
        "cluster": cluster,
        "job_id": source_job_id,
        "mcp_result_artifact": {
            "artifact_id": source_artifact_id,
            "job_id": source_job_id,
            "kind": "mcp_result",
            "sha256": source_sha256,
        },
        "service_runtime_bindings": [handoff],
    }
    loopback_urls = {
        name: f"http://127.0.0.1:19041/{name.removesuffix('_url')}"
        for name in (
            "connect_url",
            "health_url",
            "stream_url",
            "events_url",
            "state_url",
            "command_url",
        )
    }
    bind_result: dict[str, Any] = {
        "gateway_session_id": gateway_session_id,
        "gateway_session": ready_session.model_dump(mode="json"),
        **loopback_urls,
        "scheduler_cancel_requested": False,
    }
    mcp_calls: list[tuple[str, dict[str, Any]]] = []
    query_attempts = 0

    def packaged_session(
        *,
        profile: str,
        tool: str,
        arguments: dict[str, Any],
        timeout_seconds: float,
        extra_environment: dict[str, str] | None = None,
        require_enforceable_containment: bool = False,
    ) -> PackagedMcpStdioSession:
        nonlocal query_attempts
        assert profile == "user"
        assert timeout_seconds > 0
        assert require_enforceable_containment is True
        if tool == "relay_bind_jarvis_runtime":
            assert extra_environment == {
                "CLIO_RELAY_FRP_TOKEN": "transport-token-value-41",
                "CLIO_RELAY_STCP_SECRET": "transport-secret-value-41",
            }
        else:
            assert extra_environment is None
        mcp_calls.append((tool, arguments))
        payload: dict[str, Any]
        if tool == "jarvis_get_execution":
            query_attempts += 1
            if query_attempts == 1:
                payload = dict(query_result)
                payload["service_runtime_bindings"] = list[dict[str, Any]]()
            else:
                payload = query_result
        else:
            payload = bind_result
        return _secure_runtime_packaged_session(
            tool,
            payload,
            transcript_sha256=("1" if tool == "jarvis_get_execution" else "2") * 64,
        )

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_packaged_mcp_stdio_session",
        packaged_session,
    )

    class EmptyGatewayQueue:
        def list_gateway_sessions_page(
            self,
            *,
            cursor: int,
            limit: int,
            cluster: str | None = None,
        ) -> tuple[list[GatewaySession], int | None, int]:
            del cursor, limit, cluster
            return [], None, 0

    def managed_queue(_settings: object) -> EmptyGatewayQueue:
        return EmptyGatewayQueue()

    monkeypatch.setattr("clio_relay.live_acceptance.storage_managed_queue", managed_queue)
    monkeypatch.setattr("clio_relay.live_acceptance.ServiceRuntimeSupervisor", _SecureSupervisor)
    _SecureSupervisor.reset(ready_session)

    json_calls: list[tuple[str, str]] = []
    state_revision = 4

    def paraview_state(revision: int) -> dict[str, Any]:
        """Exact released JARVIS ParaView service-state.v2 outer contract."""
        return {
            "schema_version": "jarvis.paraview.service-state.v2",
            "service_instance_id": "visualizer-service-41",
            "revision": revision,
            "execution_id": "execution-service-41",
            "dataset": {
                "descriptor": dataset_descriptor,
                "selected_timestep": 1 if revision > 4 else 0,
            },
            "pipeline": {"timestep": {"index": 1 if revision > 4 else 0}},
        }

    def browser_json(
        _url: str,
        *,
        endpoint: str,
        method: str,
        body: dict[str, Any] | None,
        timeout_seconds: float,
    ) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
        nonlocal state_revision
        assert timeout_seconds > 0
        json_calls.append((endpoint, method))
        if endpoint == "command":
            assert body == {
                "schema_version": "jarvis.paraview.command.v2",
                "command_id": "view-command-41",
                "operation": "set_timestep",
                "expected_revision": 4,
                "arguments": {"index": 1},
            }
            state_revision = 5
            document: dict[str, Any] = {
                "schema_version": "jarvis.paraview.command-result.v2",
                "command_id": "view-command-41",
                "operation": "set_timestep",
                "applied": True,
                "state": paraview_state(state_revision),
                "result": {"index": 1},
            }
        elif endpoint == "health":
            assert body is None
            document = {
                "schema_version": "jarvis.paraview.health.v1",
                "status": "ready",
                "service_instance_id": "visualizer-service-41",
                "revision": state_revision,
            }
        else:
            assert body is None
            document = paraview_state(state_revision)
        return (
            _secure_runtime_http_evidence(
                endpoint=cast(Any, endpoint),
                method=cast(Any, method),
                digest_character=str(state_revision),
                revision=state_revision,
                command_id=document.get("command_id"),
            ),
            document,
        )

    def browser_sse(
        _url: str,
        *,
        timeout_seconds: float,
        expected_event_name: str,
    ) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
        assert timeout_seconds > 0
        assert expected_event_name == "state"
        document = paraview_state(state_revision)
        return (
            _secure_runtime_http_evidence(
                endpoint="events",
                method="GET",
                digest_character=str(state_revision),
                revision=state_revision,
                command_id=None,
            ),
            document,
        )

    def changed_sse(
        _url: str,
        *,
        previous: SecureRuntimeHttpEvidence,
        require_change: bool,
        timeout_seconds: float,
        poll_seconds: float,
        expected_event_name: str,
    ) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
        del previous, require_change, timeout_seconds, poll_seconds
        assert expected_event_name == "state"
        document = paraview_state(5)
        return (
            _secure_runtime_http_evidence(
                endpoint="events",
                method="GET",
                digest_character="5",
                revision=5,
                command_id=None,
            ),
            document,
        )

    def changed_state(
        _url: str,
        *,
        previous: SecureRuntimeHttpEvidence,
        require_change: bool,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
        del previous, require_change, timeout_seconds, poll_seconds
        document = paraview_state(5)
        return (
            _secure_runtime_http_evidence(
                endpoint="state",
                method="GET",
                digest_character="6",
                revision=5,
                command_id=None,
            ),
            document,
        )

    revoked_urls: list[str] = []

    def revoke(url: str, *, timeout_seconds: float, proxy_stopped: bool) -> None:
        assert timeout_seconds > 0
        assert proxy_stopped is True
        revoked_urls.append(url)

    monkeypatch.setattr("clio_relay.live_acceptance._browser_json_observation", browser_json)
    monkeypatch.setattr("clio_relay.live_acceptance._browser_sse_observation", browser_sse)
    monkeypatch.setattr(
        "clio_relay.live_acceptance._wait_for_changed_sse_event",
        changed_sse,
    )
    monkeypatch.setattr(
        "clio_relay.live_acceptance._wait_for_changed_browser_state",
        changed_state,
    )
    monkeypatch.setattr(
        "clio_relay.live_acceptance._assert_browser_capability_revoked",
        revoke,
    )

    report = new_live_validation_report(
        scenario="released-secure-runtime",
        cluster=cluster,
        launcher="uv-tool",
        install_source="pypi:clio-relay",
        artifact_sha256="d" * 64,
    )
    recorder = ValidationRecorder(report)
    forbidden = _verify_secure_runtime_acceptance(
        LiveAcceptanceOptions(
            cluster=cluster,
            definition=ClusterDefinition(name=cluster, ssh_host="edge-west.invalid"),
            timeout_seconds=30,
            poll_seconds=0.01,
            transport_token="transport-token-value-41",
            transport_secret_key="transport-secret-value-41",
        ),
        config=SecureRuntimeProbeConfig(
            package_name="builtin.paraview",
            package_id="paraview-7",
            command={
                "schema_version": "jarvis.paraview.command.v2",
                "command_id": "view-command-41",
                "operation": "set_timestep",
                "expected_revision": 4,
                "arguments": {"index": 1},
            },
            protocol_adapter=_secure_runtime_protocol_adapter(),
        ),
        runtime_metadata={
            "pipeline_id": "pipeline-service-41",
            "execution_id": "execution-service-41",
        },
        recorder=recorder,
    )
    recorder.finish()

    assert [name for name, _arguments in mcp_calls] == [
        "jarvis_get_execution",
        "jarvis_get_execution",
        "relay_bind_jarvis_runtime",
    ]
    for _name, query_arguments in mcp_calls[:2]:
        assert query_arguments["cluster"] == cluster
        assert query_arguments["pipeline_id"] == "pipeline-service-41"
        assert query_arguments["execution_id"] == "execution-service-41"
        assert query_arguments["include_service_runtimes"] is True
        assert query_arguments["wait_for_terminal"] is True
        assert 0 < query_arguments["wait_timeout_seconds"] <= 30
        assert query_arguments["poll_seconds"] == 0.01
    assert mcp_calls[2][1]["binding"] == handoff
    assert _SecureSupervisor.calls == [
        "browser_attach",
        "browser_detach",
        "detach",
        "attach",
        "browser_attach",
        "browser_detach",
        "stop:false",
    ]
    assert len(revoked_urls) == 5
    assert ("health", "GET") in json_calls
    assert ("state", "GET") in json_calls
    assert ("command", "POST") in json_calls
    assert report.status.value == "passed"
    assert report.cleanup.cancel_scheduler_jobs is False
    assert report.cleanup.remaining_resources == []
    evidence = next(
        reference
        for check in report.checks
        for reference in check.evidence
        if reference.kind == "secure_runtime_acceptance"
    )
    metadata = evidence.metadata
    readiness_evidence = next(
        reference
        for check in report.checks
        for reference in check.evidence
        if reference.reference == "packaged-mcp://jarvis_get_execution/readiness-attempt/1"
    )
    assert readiness_evidence.metadata["ready_binding_count"] == 0
    assert metadata["cluster"] == cluster
    assert metadata["lifecycle_states"] == ["ready", "degraded", "ready", "closed"]
    assert metadata["scheduler_cancel_requested"] is False
    assert metadata["claim_scope"] == "clio-relay-core-lifecycle-and-public-evidence"
    assert metadata["browser_capability_in_public_evidence"] is False
    assert metadata["raw_authority_material_in_public_evidence"] is False
    assert metadata["secret_values_absent_from_public_evidence"] is True
    binding_resource = next(
        resource for resource in report.resources if resource.kind == "secure_runtime_binding"
    )
    assert binding_resource.state == "ready"
    assert binding_resource.metadata["source_relay_artifact_sha256"] == source_sha256
    assert (
        binding_resource.metadata["evidence_scope"]
        == "clio-relay-core-lifecycle-and-public-evidence"
    )
    assert binding_resource.metadata["query_mcp_containment_enforceable"] is True
    assert binding_resource.metadata["bind_mcp_containment_enforceable"] is True
    gateway_resource = next(
        resource
        for resource in report.resources
        if resource.kind == "gateway_session" and resource.resource_id == gateway_session_id
    )
    assert gateway_resource.state == "closed"
    final_resources = [
        resource
        for resource in report.resources
        if resource.kind in {"connector", "gateway_session", "scheduler_job"}
    ]
    assert sum(resource.kind == "connector" for resource in final_resources) == 2
    for resource in final_resources:
        assert resource.metadata["cancel_scheduler_job"] is False
        assert resource.metadata["ownership_verified"] is True
        assert resource.metadata["verified_after_operation"] is True
        assert resource.metadata["residual"] is False
    assert len(metadata["browser_attachment_ids"]) == 2
    _assert_secret_free_document(
        report.model_dump(mode="json"),
        forbidden_values=forbidden,
        label="finished secure runtime report",
    )


def test_secure_runtime_ready_binding_wait_is_bounded(
    monkeypatch: MonkeyPatch,
) -> None:
    """A running execution with no ready service fails on the acceptance deadline."""
    cluster = "operator-readiness-timeout"
    calls = 0
    clock = 100.0

    def monotonic() -> float:
        return clock

    def sleep(seconds: float) -> None:
        nonlocal clock
        clock += seconds

    def packaged_session(
        *,
        profile: str,
        tool: str,
        arguments: dict[str, Any],
        timeout_seconds: float,
        extra_environment: dict[str, str] | None = None,
        require_enforceable_containment: bool = False,
    ) -> PackagedMcpStdioSession:
        nonlocal calls
        del profile, arguments, timeout_seconds, extra_environment
        assert tool == "jarvis_get_execution"
        assert require_enforceable_containment is True
        calls += 1
        return _secure_runtime_packaged_session(
            tool,
            {
                "terminal": True,
                "state": "succeeded",
                "cluster": cluster,
                "job_id": "job_readiness_timeout",
                "mcp_result_artifact": {
                    "artifact_id": "artifact_readiness_timeout",
                    "job_id": "job_readiness_timeout",
                    "kind": "mcp_result",
                    "sha256": "a" * 64,
                },
                "service_runtime_bindings": [],
            },
            transcript_sha256="9" * 64,
        )

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_packaged_mcp_stdio_session",
        packaged_session,
    )
    monkeypatch.setattr("clio_relay.live_acceptance.time.monotonic", monotonic)
    monkeypatch.setattr("clio_relay.live_acceptance.time.sleep", sleep)
    recorder = ValidationRecorder(
        new_live_validation_report(
            scenario="secure-runtime-readiness-timeout",
            cluster=cluster,
            launcher="uv-tool",
            install_source="pypi:clio-relay",
            artifact_sha256="8" * 64,
        )
    )

    with pytest.raises(
        RelayError,
        match="timed out waiting for one ready JARVIS service runtime binding",
    ) as pending:
        _verify_secure_runtime_acceptance(
            LiveAcceptanceOptions(
                cluster=cluster,
                definition=ClusterDefinition(name=cluster, ssh_host="timeout.invalid"),
                timeout_seconds=1,
                poll_seconds=1,
                transport_token="transport-token-timeout",
                transport_secret_key="transport-secret-timeout",
            ),
            config=SecureRuntimeProbeConfig(
                package_name="builtin.paraview",
                command={"command_id": "timeout-command"},
                protocol_adapter=_secure_runtime_protocol_adapter(),
            ),
            runtime_metadata={
                "pipeline_id": "pipeline-readiness-timeout",
                "execution_id": "execution-readiness-timeout",
            },
            recorder=recorder,
        )

    assert calls == 1
    pending_observation = cast(_AcceptanceObservationPending, pending.value)
    assert pending_observation.phase == "secure_runtime_query"
    assert pending_observation.identifiers == {
        "pipeline_id": "pipeline-readiness-timeout",
        "execution_id": "execution-readiness-timeout",
    }


def test_secure_runtime_pending_bind_retains_exact_gateway_without_urls() -> None:
    """A bind observation boundary exposes only its durable resume identity."""
    handoff = {
        "cluster": "operator-pending-bind",
        "source_job_id": "job_pending_bind",
        "source_artifact_id": "artifact_pending_bind",
        "package_id": "paraview-pending",
        "package_name": "builtin.paraview",
        "service_instance_id": "visualizer-pending",
    }
    gateway_session_id = "gateway_pending_bind"
    gateway = GatewaySession(
        session_id=gateway_session_id,
        cluster=handoff["cluster"],
        name="pending-viewer",
        state=GatewaySessionState.STARTING,
        scheduler="slurm",
        scheduler_job_id="99123",
        gateway={
            "jarvis_runtime_binding": {
                "source_relay_job_id": handoff["source_job_id"],
                "source_relay_artifact_id": handoff["source_artifact_id"],
                "package_id": handoff["package_id"],
                "package_name": handoff["package_name"],
                "service_instance_id": handoff["service_instance_id"],
            }
        },
        metadata={"owner": "clio-relay"},
    )
    result = {
        "outcome": "pending",
        "gateway_session_id": gateway_session_id,
        "gateway_session": gateway.model_dump(mode="json"),
        "retry_selector": {
            "cluster": handoff["cluster"],
            "gateway_session_id": gateway_session_id,
            "scheduler_provider": "slurm",
            "scheduler_job_id": "99123",
        },
        "scheduler_action": "none",
        "relay_action": "none",
        "scheduler_cancel_requested": False,
        "connect_url": None,
        "health_url": None,
        "stream_url": None,
        "events_url": None,
        "state_url": None,
        "command_url": None,
    }

    observed = _validated_secure_runtime_pending_bind(
        result,
        handoff=JarvisServiceRuntimeHandoff.model_validate(handoff),
    )

    assert observed == gateway_session_id


def test_secure_runtime_late_ready_binding_fails_deadline(
    monkeypatch: MonkeyPatch,
) -> None:
    """A ready result returned after the advertised deadline is not accepted."""
    cluster = "operator-late-ready"
    clock = 100.0
    calls = 0
    handoff = {
        "cluster": cluster,
        "source_job_id": "job_late_ready",
        "source_artifact_id": "artifact_late_ready",
        "package_id": "paraview-late-ready",
        "package_name": "builtin.paraview",
        "service_instance_id": "visualizer-late-ready",
    }

    def monotonic() -> float:
        return clock

    def packaged_session(
        *,
        profile: str,
        tool: str,
        arguments: dict[str, Any],
        timeout_seconds: float,
        extra_environment: dict[str, str] | None = None,
        require_enforceable_containment: bool = False,
    ) -> PackagedMcpStdioSession:
        nonlocal calls, clock
        del profile, arguments, timeout_seconds, extra_environment
        assert tool == "jarvis_get_execution"
        assert require_enforceable_containment is True
        calls += 1
        clock = 101.0
        return _secure_runtime_packaged_session(
            tool,
            {
                "terminal": True,
                "state": "succeeded",
                "cluster": cluster,
                "job_id": handoff["source_job_id"],
                "mcp_result_artifact": {
                    "artifact_id": handoff["source_artifact_id"],
                    "job_id": handoff["source_job_id"],
                    "kind": "mcp_result",
                    "sha256": "d" * 64,
                },
                "service_runtime_bindings": [handoff],
            },
            transcript_sha256="e" * 64,
        )

    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_packaged_mcp_stdio_session",
        packaged_session,
    )
    monkeypatch.setattr("clio_relay.live_acceptance.time.monotonic", monotonic)
    recorder = ValidationRecorder(
        new_live_validation_report(
            scenario="secure-runtime-late-ready",
            cluster=cluster,
            launcher="uv-tool",
            install_source="pypi:clio-relay",
            artifact_sha256="f" * 64,
        )
    )

    with pytest.raises(
        RelayError,
        match="timed out waiting for one ready JARVIS service runtime binding",
    ):
        _verify_secure_runtime_acceptance(
            LiveAcceptanceOptions(
                cluster=cluster,
                definition=ClusterDefinition(name=cluster, ssh_host="late-ready.invalid"),
                timeout_seconds=1,
                poll_seconds=0.1,
                transport_token="transport-token-late-ready",
                transport_secret_key="transport-secret-late-ready",
            ),
            config=SecureRuntimeProbeConfig(
                package_name="builtin.paraview",
                package_id="paraview-late-ready",
                command={"command_id": "late-ready-command"},
                protocol_adapter=_secure_runtime_protocol_adapter(),
            ),
            runtime_metadata={
                "pipeline_id": "pipeline-late-ready",
                "execution_id": "execution-late-ready",
            },
            recorder=recorder,
        )

    assert calls == 1


def test_secure_runtime_ready_binding_ambiguity_fails_immediately() -> None:
    """Multiple matching ready services are an identity error, not a retry state."""
    cluster = "operator-ambiguous-runtime"
    handoff = {
        "cluster": cluster,
        "source_job_id": "job_ambiguous_runtime",
        "source_artifact_id": "artifact_ambiguous_runtime",
        "package_id": "paraview-ambiguous",
        "package_name": "builtin.paraview",
        "service_instance_id": "visualizer-ambiguous-a",
    }
    config = SecureRuntimeProbeConfig(
        package_name="builtin.paraview",
        package_id="paraview-ambiguous",
        command={"command_id": "ambiguous-command"},
        protocol_adapter=_secure_runtime_protocol_adapter(),
    )

    with pytest.raises(RelayError, match="matched=2"):
        _select_secure_runtime_handoff(
            {
                "terminal": True,
                "state": "succeeded",
                "cluster": cluster,
                "job_id": "job_ambiguous_runtime",
                "mcp_result_artifact": {
                    "artifact_id": "artifact_ambiguous_runtime",
                    "job_id": "job_ambiguous_runtime",
                    "kind": "mcp_result",
                    "sha256": "b" * 64,
                },
                "service_runtime_bindings": [
                    handoff,
                    {**handoff, "service_instance_id": "visualizer-ambiguous-b"},
                ],
            },
            cluster=cluster,
            config=config,
        )


def test_secure_runtime_empty_binding_rejects_cluster_identity_drift() -> None:
    """An empty binding list is transient only for the exact requested cluster."""
    cluster = "operator-exact-runtime"
    with pytest.raises(RelayError, match="changed cluster identity"):
        _select_secure_runtime_handoff(
            {
                "terminal": True,
                "state": "succeeded",
                "cluster": "operator-wrong-runtime",
                "job_id": "job_exact_runtime",
                "mcp_result_artifact": {
                    "artifact_id": "artifact_exact_runtime",
                    "job_id": "job_exact_runtime",
                    "kind": "mcp_result",
                    "sha256": "c" * 64,
                },
                "service_runtime_bindings": [],
            },
            cluster=cluster,
            config=SecureRuntimeProbeConfig(
                package_name="builtin.paraview",
                command={"command_id": "exact-command"},
                protocol_adapter=_secure_runtime_protocol_adapter(),
            ),
        )


def test_secure_runtime_bind_failure_preserves_error_and_attempts_safe_teardown(
    monkeypatch: MonkeyPatch,
) -> None:
    """A lost bind response recovers its exact new gateway and annotates failed cleanup."""
    cluster = "operator-lab-two"
    handoff = {
        "cluster": cluster,
        "source_job_id": "job_bind_failure",
        "source_artifact_id": "artifact_bind_failure",
        "package_id": "service-2",
        "package_name": "site.runtime-service",
        "service_instance_id": "runtime-service-2",
    }
    descriptor = _secure_runtime_dataset_descriptor()
    descriptor_sha256 = _secure_runtime_json_sha256(descriptor)
    binding = {
        "schema_version": "clio-relay.jarvis-service-runtime-binding.v2",
        "source_relay_job_id": handoff["source_job_id"],
        "source_relay_artifact_id": handoff["source_artifact_id"],
        "source_relay_artifact_sha256": "4" * 64,
        "source_tool": "jarvis_get_execution",
        "jarvis_execution_id": "execution-bind-failure",
        "scheduler_provider": "slurm",
        "scheduler_native_id": "902",
        "package_id": handoff["package_id"],
        "package_name": handoff["package_name"],
        "service_instance_id": handoff["service_instance_id"],
        "service_revision": 1,
        "service_report_sha256": "5" * 64,
        "service_runtime_schema_version": "jarvis.service-runtime.v2",
        "authorization_sha256": "6" * 64,
        "dataset_descriptor_sha256": descriptor_sha256,
        "dataset_descriptor": descriptor,
    }
    session = GatewaySession(
        session_id="gateway_bind_failure",
        cluster=cluster,
        name="failure-runtime",
        state=GatewaySessionState.READY,
        scheduler="slurm",
        scheduler_job_id="902",
        gateway={"jarvis_runtime_binding": binding},
        metadata={"owner": "clio-relay"},
    )
    query_result = {
        "terminal": True,
        "state": "succeeded",
        "cluster": cluster,
        "job_id": handoff["source_job_id"],
        "mcp_result_artifact": {
            "artifact_id": handoff["source_artifact_id"],
            "job_id": handoff["source_job_id"],
            "kind": "mcp_result",
            "sha256": "4" * 64,
        },
        "service_runtime_bindings": [handoff],
    }
    bind_state = {"created": False}

    def packaged_session(
        *,
        profile: str,
        tool: str,
        arguments: dict[str, Any],
        timeout_seconds: float,
        extra_environment: dict[str, str] | None = None,
        require_enforceable_containment: bool = False,
    ) -> PackagedMcpStdioSession:
        del profile, arguments, timeout_seconds, extra_environment
        assert require_enforceable_containment is True
        if tool != "jarvis_get_execution":
            bind_state["created"] = True
            raise RelayError("simulated lost bind response")
        return _secure_runtime_packaged_session(
            tool,
            query_result,
            transcript_sha256="7" * 64,
        )

    class EmptyGatewayQueue:
        def list_gateway_sessions_page(
            self,
            *,
            cursor: int,
            limit: int,
            cluster: str | None = None,
        ) -> tuple[list[GatewaySession], int | None, int]:
            del cursor, limit, cluster
            return ([session], None, 1) if bind_state["created"] else ([], None, 0)

    class FailingCleanupSupervisor(_SecureSupervisor):
        def stop(
            self,
            *,
            session_id: str,
            cancel_scheduler_job: bool = False,
        ) -> ServiceRuntimeStopResult:
            assert session_id == session.session_id
            type(self).calls.append(f"stop:{str(cancel_scheduler_job).lower()}")
            raise RelayError("simulated cleanup failure")

    def managed_queue(_settings: object) -> EmptyGatewayQueue:
        return EmptyGatewayQueue()

    FailingCleanupSupervisor.reset(session)
    monkeypatch.setattr(
        "clio_relay.live_acceptance.run_packaged_mcp_stdio_session",
        packaged_session,
    )
    monkeypatch.setattr("clio_relay.live_acceptance.storage_managed_queue", managed_queue)
    monkeypatch.setattr(
        "clio_relay.live_acceptance.ServiceRuntimeSupervisor",
        FailingCleanupSupervisor,
    )
    recorder = ValidationRecorder(
        new_live_validation_report(scenario="secure-runtime", cluster=cluster)
    )

    with pytest.raises(RelayError, match="simulated lost bind response") as failure:
        _verify_secure_runtime_acceptance(
            LiveAcceptanceOptions(
                cluster=cluster,
                definition=ClusterDefinition(name=cluster, ssh_host="lab-two.invalid"),
                timeout_seconds=30,
                poll_seconds=0.01,
                transport_token="bind-failure-token",
                transport_secret_key="bind-failure-secret",
            ),
            config=SecureRuntimeProbeConfig(
                package_name="site.runtime-service",
                package_id="service-2",
                command={"command_id": "probe-bind-failure", "action": "probe"},
                protocol_adapter=_secure_runtime_protocol_adapter(),
            ),
            runtime_metadata={
                "pipeline_id": "pipeline-bind-failure",
                "execution_id": "execution-bind-failure",
            },
            recorder=recorder,
        )

    assert FailingCleanupSupervisor.calls == ["stop:false"]
    assert any(
        "simulated cleanup failure" in note for note in getattr(failure.value, "__notes__", [])
    )


def test_packaged_mcp_acceptance_evidence_projects_untrusted_server_info() -> None:
    """Unknown initialize fields remain process-local and never enter live reports."""
    secret = "one-time-server-capability-secret"
    session = _secure_runtime_packaged_session(
        "jarvis_get_execution",
        {"job_id": "job-1"},
        transcript_sha256="7" * 64,
        server_info_extra={"capability": secret},
    )

    evidence = _packaged_mcp_acceptance_evidence(
        session,
        expected_tool="jarvis_get_execution",
    )
    serialized = evidence.model_dump_json()

    assert evidence.server_name == "clio-relay"
    assert evidence.server_version == "test-wheel"
    assert "capability" not in serialized
    assert secret not in serialized


def _secure_runtime_dataset_descriptor() -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema_version": "jarvis.dataset-descriptor.v1",
        "dataset_id": "custom-visualizer-dataset",
        "kind": "temporal-field",
        "format": "site-native",
        "members": [{"index": 0, "location": "/datasets/site/run-41/output.bin"}],
        "arrays": [],
        "bounds": None,
        "source_artifact": None,
    }
    digest = hashlib.sha256(
        json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    return {**document, "fingerprint": {"algorithm": "sha256", "digest": digest}}


def _secure_runtime_json_sha256(document: object) -> str:
    return hashlib.sha256(
        json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _secure_runtime_protocol_adapter() -> SecureRuntimeProtocolAdapter:
    """Return the application-owned selectors for the released ParaView v2 protocol."""
    state_selectors = {
        "service_instance_id_pointer": "/service_instance_id",
        "execution_id_pointer": "/execution_id",
        "dataset_descriptor_pointer": "/dataset/descriptor",
        "revision_pointer": "/revision",
    }
    return SecureRuntimeProtocolAdapter.model_validate(
        {
            "command_request_id_pointer": "/command_id",
            "health": {
                "assertions": {
                    "/schema_version": "jarvis.paraview.health.v1",
                    "/status": "ready",
                },
                "service_instance_id_pointer": "/service_instance_id",
                "revision_pointer": "/revision",
            },
            "state": {
                "assertions": {"/schema_version": "jarvis.paraview.service-state.v2"},
                **state_selectors,
            },
            "command": {
                "assertions": {
                    "/schema_version": "jarvis.paraview.command-result.v2",
                    "/applied": True,
                },
                "service_instance_id_pointer": "/state/service_instance_id",
                "execution_id_pointer": "/state/execution_id",
                "dataset_descriptor_pointer": "/state/dataset/descriptor",
                "revision_pointer": "/state/revision",
                "command_id_pointer": "/command_id",
            },
            "events": {
                "assertions": {"/schema_version": "jarvis.paraview.service-state.v2"},
                **state_selectors,
                "event_name": "state",
            },
        }
    )


def _secure_runtime_packaged_session(
    tool: str,
    payload: dict[str, Any],
    *,
    transcript_sha256: str,
    server_info_extra: dict[str, Any] | None = None,
) -> PackagedMcpStdioSession:
    result = {
        "content": [
            {
                "type": "text",
                "text": json.dumps(payload, sort_keys=True, separators=(",", ":")),
            }
        ],
        "structuredContent": payload,
        "isError": False,
    }
    server_info = {"name": "clio-relay", "version": "test-wheel"}
    if server_info_extra is not None:
        server_info.update(server_info_extra)
    tools = [
        {
            "name": tool,
            "description": f"Observed {tool} schema",
            "inputSchema": {"type": "object", "additionalProperties": False},
        }
    ]
    executable = str((Path.cwd() / "installed" / "clio-relay").resolve())
    return PackagedMcpStdioSession(
        command=(executable, "mcp-server", "--profile", "user"),
        returncode=0,
        initialize_response={
            "result": {"protocolVersion": "2024-11-05", "serverInfo": server_info}
        },
        tools_list_response={"result": {"tools": tools}},
        tools_call_response={"result": result},
        transcript_sha256=transcript_sha256,
        stderr_sha256="0" * 64,
        stderr_excerpt="",
        configured_executable=executable,
        canonical_executable=executable,
        executable_sha256="3" * 64,
        server_info_sha256=_secure_runtime_json_sha256(server_info),
        tools_list_sha256=_secure_runtime_json_sha256({"tools": tools}),
        called_tool_schema_sha256=_secure_runtime_json_sha256(tools[0]),
        jarvis_virtual_tools_sha256="4" * 64,
        called_tool_name=tool,
        containment_mode="windows_job_object",
        containment_enforceable=True,
    )


def _secure_runtime_http_evidence(
    *,
    endpoint: Any,
    method: Any,
    digest_character: str,
    revision: int | None = None,
    command_id: object = None,
) -> SecureRuntimeHttpEvidence:
    return SecureRuntimeHttpEvidence(
        endpoint=endpoint,
        method=method,
        status_code=200,
        content_type=("text/event-stream" if endpoint == "events" else "application/json"),
        body_sha256=digest_character * 64,
        body_bytes=32,
        service_instance_id="visualizer-service-41",
        execution_id="execution-service-41",
        dataset_descriptor_sha256=_secure_runtime_json_sha256(_secure_runtime_dataset_descriptor()),
        command_id=command_id if isinstance(command_id, str) else None,
        revision=revision,
    )


def _secure_runtime_cleanup_resource(
    *,
    kind: str,
    resource_id: str,
    action: Any,
    outcome: Any,
    gateway_session_id: str,
    provider: str | None = None,
    observed_state: str | None = None,
) -> CleanupResource:
    return CleanupResource(
        kind=kind,
        resource_id=resource_id,
        location=f"runtime://{gateway_session_id}/{kind}/{resource_id}",
        action=action,
        ownership_verified=True,
        outcome=outcome,
        provider=provider,
        verified_after_operation=True,
        observed_state=observed_state,
        metadata={"gateway_session_id": gateway_session_id},
    )


class _SecureSupervisor:
    session: GatewaySession
    calls: list[str] = []
    attachment_counter: int = 0

    def __init__(self, **_kwargs: object) -> None:
        pass

    @classmethod
    def reset(cls, session: GatewaySession) -> None:
        cls.session = session
        cls.calls = []
        cls.attachment_counter = 0

    def browser_attach(self, *, session_id: str, ttl_seconds: int) -> BrowserAttachmentGrant:
        assert session_id == self.session.session_id
        assert ttl_seconds == 300
        type(self).calls.append("browser_attach")
        type(self).attachment_counter += 1
        counter = type(self).attachment_counter
        capability = ("e" if counter == 1 else "f") * 64
        base = f"http://127.0.0.1:{19100 + counter}"

        def url(path: str) -> str:
            return f"{base}/{path}?capability={capability}"

        return BrowserAttachmentGrant(
            attachment_id=f"browser-secure-{counter}",
            expires_at="2026-07-19T22:00:00Z",
            connect_url=url("connect"),
            health_url=url("health"),
            stream_url=url("stream"),
            events_url=url("events"),
            state_url=url("state"),
            command_url=url("commands"),
        )

    def browser_detach(
        self,
        *,
        session_id: str,
        attachment_id: str,
    ) -> BrowserDetachmentResult:
        assert session_id == self.session.session_id
        type(self).calls.append("browser_detach")
        return BrowserDetachmentResult(
            attachment_id=attachment_id,
            revoked_at="2026-07-19T21:00:00Z",
            already_revoked=False,
            proxy_process_id=911,
            proxy_stopped=True,
        )

    def detach(self, *, session_id: str) -> ServiceRuntimeStopResult:
        assert session_id == self.session.session_id
        type(self).calls.append("detach")
        self.session = self.session.model_copy(update={"state": GatewaySessionState.DEGRADED})
        return ServiceRuntimeStopResult(
            session=self.session,
            mode="detach",
            stopped_local_pid=101,
            stopped_remote_pid=None,
            canceled_scheduler_job=None,
            resources=self._cleanup_resources(mode="detach"),
            errors=[],
        )

    def attach(self, *, session_id: str) -> ServiceRuntimeStartResult:
        assert session_id == self.session.session_id
        type(self).calls.append("attach")
        self.session = self.session.model_copy(update={"state": GatewaySessionState.READY})
        return ServiceRuntimeStartResult(
            session=self.session,
            connect_url="http://127.0.0.1:19041/connect",
            health_url="http://127.0.0.1:19041/health",
            stream_url="http://127.0.0.1:19041/stream",
            compatibility_urls={},
            events_url="http://127.0.0.1:19041/events",
            state_url="http://127.0.0.1:19041/state",
            command_url="http://127.0.0.1:19041/commands",
        )

    def stop(
        self,
        *,
        session_id: str,
        cancel_scheduler_job: bool = False,
    ) -> ServiceRuntimeStopResult:
        assert session_id == self.session.session_id
        type(self).calls.append(f"stop:{str(cancel_scheduler_job).lower()}")
        self.session = self.session.model_copy(
            update={
                "state": GatewaySessionState.CLOSED,
                "gateway": {
                    **self.session.gateway,
                    "teardown_intent": {
                        "schema_version": "clio-relay.gateway-teardown-intent.v1",
                        "operation_id": "gateway_cleanup_secure_runtime_41",
                        "gateway_session_id": self.session.session_id,
                        "cancel_scheduler_job": False,
                        "created_at": "2026-07-19T21:00:00Z",
                    },
                },
            }
        )
        return ServiceRuntimeStopResult(
            session=self.session,
            mode="teardown",
            stopped_local_pid=102,
            stopped_remote_pid=202,
            canceled_scheduler_job=None,
            resources=self._cleanup_resources(mode="teardown"),
            errors=[],
        )

    def _cleanup_resources(self, *, mode: str) -> list[CleanupResource]:
        session_id = self.session.session_id
        if mode == "detach":
            connector_actions = (("desktop_connector", "stop", "stopped"),)
            remote_action = ("remote_connector", "retain", "retained")
            gateway_action = ("gateway_record", "retain", "retained")
        else:
            connector_actions = (
                ("desktop_connector", "stop", "stopped"),
                ("remote_connector", "stop", "stopped"),
            )
            remote_action = None
            gateway_action = ("gateway_record", "close", "closed")
        scheduler_outcome = "retained" if mode == "detach" else "terminal"
        scheduler_state = "running" if mode == "detach" else "completed"
        resources = [
            _secure_runtime_cleanup_resource(
                kind=kind,
                resource_id=f"{kind}-41",
                action=action,
                outcome=outcome,
                gateway_session_id=session_id,
            )
            for kind, action, outcome in connector_actions
        ]
        if remote_action is not None:
            kind, action, outcome = remote_action
            resources.append(
                _secure_runtime_cleanup_resource(
                    kind=kind,
                    resource_id="remote_connector-41",
                    action=action,
                    outcome=outcome,
                    gateway_session_id=session_id,
                )
            )
        resources.extend(
            [
                _secure_runtime_cleanup_resource(
                    kind="scheduler_job",
                    resource_id="90141",
                    action="retain",
                    outcome=scheduler_outcome,
                    gateway_session_id=session_id,
                    provider="slurm",
                    observed_state=scheduler_state,
                ),
                _secure_runtime_cleanup_resource(
                    kind=gateway_action[0],
                    resource_id=session_id,
                    action=gateway_action[1],
                    outcome=gateway_action[2],
                    gateway_session_id=session_id,
                ),
            ]
        )
        return resources

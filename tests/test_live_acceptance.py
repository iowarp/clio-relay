from __future__ import annotations

import json
import subprocess
import urllib.request
from base64 import b64encode
from pathlib import Path
from typing import cast

import pytest
from pytest import MonkeyPatch

from clio_relay.cluster_config import ClusterDefinition, LiveTestConfig
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.live_acceptance import (
    CommandRunner,
    LiveAcceptanceOptions,
    _assert_progress_adapter,  # pyright: ignore[reportPrivateUsage]
    _expected_progress_adapter,  # pyright: ignore[reportPrivateUsage]
    _expected_progress_package,  # pyright: ignore[reportPrivateUsage]
    _find_agent_child_job,  # pyright: ignore[reportPrivateUsage]
    _http_json,  # pyright: ignore[reportPrivateUsage]
    _verify_cluster_deployment,  # pyright: ignore[reportPrivateUsage]
    _verify_live_package_progress,  # pyright: ignore[reportPrivateUsage]
    _verify_runtime_metadata_artifact,  # pyright: ignore[reportPrivateUsage]
    run_live_acceptance,
)
from clio_relay.validation_report import (
    TransportCleanupResourceEvidence,
    TransportProbeEvidence,
    load_validation_report,
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

    assert structured is False
    assert "acceptance.structured_runtime_metadata=ok" not in lines
    assert "acceptance.structured_runtime_scheduler_identity=ok" not in lines
    assert "runtime_metadata.compatibility=acceptance:untrusted_compatibility" in lines


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

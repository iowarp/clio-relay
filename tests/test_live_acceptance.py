from __future__ import annotations

import json
import subprocess
from base64 import b64encode
from pathlib import Path

import pytest
from pytest import MonkeyPatch

from clio_relay.cluster_config import ClusterDefinition, LiveTestConfig
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.live_acceptance import (
    CommandRunner,
    LiveAcceptanceOptions,
    _assert_progress_adapter,  # pyright: ignore[reportPrivateUsage]
    _expected_progress_adapter,  # pyright: ignore[reportPrivateUsage]
    _find_agent_child_job,  # pyright: ignore[reportPrivateUsage]
    _verify_live_package_progress,  # pyright: ignore[reportPrivateUsage]
    run_live_acceptance,
)


def test_live_acceptance_requires_configured_workload() -> None:
    with pytest.raises(ConfigurationError, match="live-test requires"):
        run_live_acceptance(
            LiveAcceptanceOptions(
                cluster="test-cluster",
                definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            )
        )


def test_live_acceptance_stages_files_and_strips_relay_extension(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    input_script = tmp_path / "in.lj"
    input_script.write_text("run 10\n", encoding="utf-8")
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        "name: lammps\n"
        "x_clio_relay:\n"
        "  stage_files:\n"
        "  - local_path: in.lj\n"
        "    remote_path: .local/share/clio-relay/live-tests/{run_id}/in.lj\n"
        "pkgs:\n"
        "- pkg_type: builtin.lammps\n"
        "  script: .local/share/clio-relay/live-tests/{run_id}/in.lj\n",
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
        if "builtin.builtin.lammps.pkg" in script:
            return _completed(command, "/opt/jarvis/builtin/builtin/lammps/pkg.py\n")
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
                json.dumps(
                    [
                        {
                            "metadata": {
                                "source": "jarvis_package",
                                "adapter": "lammps",
                                "package_name": "builtin.lammps",
                                "package_version": "builtin",
                                "run_id": "job_abc",
                                "execution_id": "job_abc",
                                "timing_source": "lammps_thermo_cpu",
                                "prediction_status": "observed_lammps_timing",
                                "prediction_method": "trimmed_mean_step_time_after_warmup",
                                "rate_samples": 2,
                                "eta_seconds": 1.0,
                            }
                        }
                    ]
                ),
            )
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("clio_relay.live_acceptance.run_cluster_doctor", fake_cluster_doctor)

    lines = run_live_acceptance(
        LiveAcceptanceOptions(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            jarvis_yaml=pipeline,
        ),
        runner=fake_runner,
    )

    assert (
        "acceptance.jarvis_package=builtin.lammps:/opt/jarvis/builtin/builtin/lammps/pkg.py"
        in lines
    )
    assert any(item[1] is not None and b"run 10" in item[1] for item in uploaded)
    pipeline_upload = uploaded[-1][1]
    assert pipeline_upload is not None
    assert b"x_clio_relay" not in pipeline_upload
    assert b"pkg_type: builtin.lammps" in pipeline_upload
    assert b"{run_id}" not in pipeline_upload


def test_live_acceptance_requires_trusted_package_progress() -> None:
    pipeline_yaml = "name: lammps\npkgs:\n- pkg_type: builtin.lammps\n"

    assert _expected_progress_adapter(pipeline_yaml) == "lammps"
    with pytest.raises(RelayError, match="expected package progress adapter"):
        _assert_progress_adapter(
            [
                {
                    "metadata": {
                        "adapter": "lammps",
                        "source": "external",
                        "package_name": "builtin.lammps",
                    }
                }
            ],
            "lammps",
            job_id="job_test",
        )
    with pytest.raises(RelayError, match="expected package progress adapter"):
        _assert_progress_adapter(
            [
                {
                    "metadata": {
                        "adapter": "lammps",
                        "source": "jarvis_package",
                        "package_name": "builtin.lammps",
                        "package_version": "builtin",
                        "run_id": "job_test",
                        "execution_id": "job_test",
                    }
                }
            ],
            "lammps",
            job_id="job_test",
        )
    _assert_progress_adapter(
        [
            {
                "metadata": {
                    "adapter": "lammps",
                    "source": "jarvis_package",
                    "package_name": "builtin.lammps",
                    "package_version": "builtin",
                    "run_id": "job_test",
                    "execution_id": "job_test",
                    "timing_source": "lammps_thermo_cpu",
                    "prediction_status": "observed_lammps_timing",
                    "prediction_method": "trimmed_mean_step_time_after_warmup",
                    "rate_samples": 2,
                    "eta_seconds": 10.0,
                }
            }
        ],
        "lammps",
        job_id="job_test",
    )


def test_live_acceptance_requires_single_builtin_lammps_package_for_progress() -> None:
    mixed = (
        "name: mixed\npkgs:\n- pkg_type: builtin.lammps\n- pkg_type: clio_relay.bounded_command\n"
    )

    assert _expected_progress_adapter(mixed) is None


def test_live_acceptance_rejects_package_progress_only_after_terminal_state() -> None:
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
                json.dumps(
                    [
                        {
                            "metadata": {
                                "adapter": "lammps",
                                "source": "jarvis_package",
                                "package_name": "builtin.lammps",
                                "package_version": "builtin",
                                "run_id": "job_test",
                                "execution_id": "job_test",
                                "timing_source": "lammps_thermo_cpu",
                                "prediction_status": "observed_lammps_timing",
                                "prediction_method": "trimmed_mean_step_time_after_warmup",
                                "rate_samples": 2,
                                "eta_seconds": 1.0,
                            }
                        }
                    ]
                ),
            )
        raise AssertionError(f"unexpected command: {command}")

    with pytest.raises(RelayError, match="before terminal job state"):
        _verify_live_package_progress(
            ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            "job_test",
            "lammps",
            package_name="builtin.lammps",
            timeout_seconds=1,
            poll_seconds=0.01,
            runner=fake_runner,
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
        return ["transport.healthz=ok"]

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
            uploads[script.split("cat > ", maxsplit=1)[1].strip("'")] = input
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

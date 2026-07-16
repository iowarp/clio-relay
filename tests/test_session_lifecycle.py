from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from typing import Literal

import pytest
from pytest import MonkeyPatch

import clio_relay.session_lifecycle as session_lifecycle
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle import (
    SESSION_CONNECTORS_CHECK_ID,
    SESSION_GATEWAY_CHECK_ID,
    SESSION_SCHEDULER_CANCELED_CHECK_ID,
    SESSION_WORKER_CHECK_ID,
    CleanupResource,
    RemoteSessionStateEvidence,
    SessionLifecycleReport,
    detach_remote_session,
    start_remote_session,
    status_remote_session,
    teardown_remote_session,
)


def test_scheduler_cancellation_evidence_rejects_an_extra_relay_link() -> None:
    report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        scheduler_cancel_requested=True,
        resources=[
            CleanupResource(
                kind="relay_job",
                resource_id="relay-1",
                location="ares",
                action="cancel",
                ownership_verified=True,
                outcome="canceled",
                verified_after_operation=True,
                metadata={"scheduler_job_ids": ["scheduler-1"]},
            ),
            *[
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_id,
                    location="ares",
                    action="cancel",
                    ownership_verified=True,
                    outcome="canceled",
                    verified_after_operation=True,
                    metadata={"relay_job_id": "relay-1"},
                )
                for scheduler_id in ("scheduler-1", "scheduler-unexpected")
            ],
        ],
    )

    checks = {
        check.check_id: check for check in report.to_live_validation_report(cancel_jobs=True).checks
    }

    assert checks[SESSION_SCHEDULER_CANCELED_CHECK_ID].status.value == "failed"


def test_scheduler_cancellation_evidence_rejects_a_missing_gateway_record() -> None:
    observed_at = datetime.now(UTC)
    report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        scheduler_cancel_requested=True,
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=True,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="ares",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="scheduler_job",
                resource_id="scheduler-1",
                location="ares",
                provider="slurm",
                action="cancel",
                ownership_verified=True,
                outcome="canceled",
                verified_after_operation=True,
                metadata={"gateway_session_id": "missing-gateway"},
            ),
        ],
    )

    canonical = report.to_live_validation_report()
    checks = {check.check_id: check for check in canonical.checks}

    assert checks[SESSION_SCHEDULER_CANCELED_CHECK_ID].status.value == "failed"
    assert canonical.status.value == "failed"


def test_scheduler_cancellation_evidence_accepts_a_linked_gateway_cleanup() -> None:
    observed_at = datetime.now(UTC)
    report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        scheduler_cancel_requested=True,
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=True,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="ares",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="desktop_connector",
                resource_id="desktop-connector-1",
                location="desktop",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
            CleanupResource(
                kind="remote_connector",
                resource_id="remote-connector-1",
                location="ares",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
            CleanupResource(
                kind="gateway_record",
                resource_id="gateway-1",
                location="desktop",
                action="close",
                ownership_verified=True,
                outcome="closed",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="scheduler_job",
                resource_id="scheduler-1",
                location="ares",
                provider="slurm",
                action="cancel",
                ownership_verified=True,
                outcome="canceled",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
        ],
    )

    canonical = report.to_live_validation_report()
    checks = {check.check_id: check for check in canonical.checks}

    assert checks[SESSION_SCHEDULER_CANCELED_CHECK_ID].status.value == "passed"
    assert canonical.status.value == "passed"


def test_start_remote_session_writes_owned_pid_and_metadata(monkeypatch: MonkeyPatch) -> None:
    calls: list[tuple[list[str], str]] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        calls.append((command, input.decode("utf-8")))
        assert capture_output is True
        assert check is False
        assert timeout == 120
        return subprocess.CompletedProcess(
            command,
            0,
            b"session_started=session-1\napi_pid=123\nremote_api_port=9001\n",
            b"",
        )

    monkeypatch.setattr("clio_relay.session_lifecycle.subprocess.run", fake_run)

    lines = start_remote_session(
        cluster="ares",
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        remote_api_port=9001,
        api_token="token",
    )

    assert "session_started=session-1" in lines
    assert calls[0][0] == ["ssh", "ares", "bash", "-s"]
    script = calls[0][1]
    assert "CLIO_RELAY_API_TOKEN='token'" in script
    assert "clio-relay api start --host 127.0.0.1 --port 9001 --require-token" in script
    assert "api.pid" in script
    assert "metadata.json" in script
    assert "is_owned_api_pid" in script
    assert "refusing to replace an active session API without ownership proof" in script
    assert "active session API group without a PID record" in script
    assert "/proc/{pid}/cmdline" in script
    assert "CLIO_RELAY_SESSION_OWNER_TOKEN" in script
    assert "CLIO_RELAY_OWNER_SESSION_ID=$session_id" in script
    assert "process_start_ticks" in script
    assert "nohup setsid" in script
    assert '>"$log_file" 2>&1 9>&- &' in script
    assert "umask 077" in script
    assert "trap cleanup_incomplete_start EXIT" in script
    assert "flock -w 10 -x 9" in script
    assert "CLIO_RELAY_SESSION_GENERATION_ID" in script
    assert "session_generation_id" in script
    assert "clio-relay session prepare-start" in script
    assert '--recorded-generation-id "$recorded_generation_id"' in script
    assert script.index("clio-relay session prepare-start") < script.index(
        'kill -- "-$existing_owned_pgid"'
    )
    assert "clio-relay session resume-intake" in script
    assert '--session-generation-id "$session_generation_id"' in script
    assert script.index("flock -w 10 -x 9") < script.index("clio-relay session resume-intake")
    assert 'kill -0 -- "-$existing_owned_pgid"' in script
    assert 'kill -0 -- "-$api_pid"' in script
    assert "os.replace(temporary, path)" in script
    assert 'url = f"http://127.0.0.1:{port}/healthz"' in script
    assert "owned API did not become ready" in script
    assert "\x00" not in script
    assert "pkill" not in script


def test_status_remote_session_returns_json(monkeypatch: MonkeyPatch) -> None:
    scripts: list[str] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        del capture_output, check, timeout
        scripts.append(input.decode("utf-8"))
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps({"session_id": "session-1", "running": True}).encode("utf-8"),
            b"",
        )

    monkeypatch.setattr("clio_relay.session_lifecycle.subprocess.run", fake_run)

    status = status_remote_session(
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
    )

    assert status == {"session_id": "session-1", "running": True}
    assert 'metadata.pop("owner_token", None)' in scripts[0]


def test_remote_session_command_timeout_is_reported(monkeypatch: MonkeyPatch) -> None:
    def timed_out(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        del input, capture_output, check
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr("clio_relay.session_lifecycle.subprocess.run", timed_out)

    with pytest.raises(RelayError, match="timed out after 120 seconds"):
        status_remote_session(
            definition=ClusterDefinition(name="ares", ssh_host="ares"),
            session_id="session-1",
        )


def test_detach_remote_session_retains_verified_remote_api(monkeypatch: MonkeyPatch) -> None:
    def fake_status(**_kwargs: object) -> dict[str, object]:
        return {
            "session_id": "session-1",
            "api_pid": 123,
            "running": True,
            "ownership_verified": True,
            "session_generation_id": "generation-123",
        }

    monkeypatch.setattr(
        "clio_relay.session_lifecycle.status_remote_session",
        fake_status,
    )

    report = detach_remote_session(
        definition=ClusterDefinition(name="cluster", ssh_host="cluster"),
        session_id="session-1",
        cluster="cluster",
    )

    assert report.mode == "detach"
    assert report.session_generation_id == "generation-123"
    assert report.resources[0].action == "retain"
    assert report.resources[0].outcome == "retained"
    assert report.resources[0].ownership_verified is True
    assert report.residual_resources == []
    cleanup = report.to_cleanup_evidence()
    assert cleanup.mode == "detach"
    assert cleanup.remaining_resources == []
    assert report.validation_resources()[0].kind == "relay_session"
    assert report.json_payload()["cleanup_evidence"] == cleanup.model_dump(mode="json")


@pytest.mark.parametrize(
    ("running", "ownership_verified", "expected_outcome"),
    [(False, True, "missing"), (True, False, "refused")],
)
def test_detach_remote_session_rejects_unverified_remote_api_retention(
    monkeypatch: MonkeyPatch,
    running: bool,
    ownership_verified: bool,
    expected_outcome: str,
) -> None:
    def fake_status(**_kwargs: object) -> dict[str, object]:
        return {
            "session_id": "session-1",
            "api_pid": 123,
            "running": running,
            "ownership_verified": ownership_verified,
            "session_generation_id": "generation-123",
        }

    monkeypatch.setattr("clio_relay.session_lifecycle.status_remote_session", fake_status)

    report = detach_remote_session(
        definition=ClusterDefinition(name="cluster", ssh_host="cluster"),
        session_id="session-1",
        cluster="cluster",
    )

    assert report.resources[0].outcome == expected_outcome
    assert report.resources[0].verified_after_operation is False
    assert report.resources[0].residual is True
    assert report.errors
    assert report.json_payload()["ok"] is False
    assert report.to_live_validation_report().status.value == "failed"


def test_detach_report_requires_verified_connector_and_gateway_dispositions() -> None:
    report = SessionLifecycleReport(
        cluster="cluster",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="detach",
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="cluster",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="desktop_connector",
                resource_id="456",
                location="desktop",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
            CleanupResource(
                kind="remote_connector",
                resource_id="789",
                location="cluster",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
            CleanupResource(
                kind="gateway_record",
                resource_id="gateway-1",
                location="desktop",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                observed_state="degraded",
            ),
        ],
    )

    canonical = report.to_live_validation_report()
    checks = {check.check_id: check.status.value for check in canonical.checks}

    assert checks[SESSION_CONNECTORS_CHECK_ID] == "passed"
    assert checks[SESSION_GATEWAY_CHECK_ID] == "passed"
    assert canonical.status.value == "passed"

    missing_gateway = report.model_copy(
        update={
            "resources": [
                resource for resource in report.resources if resource.kind != "gateway_record"
            ]
        }
    )
    incomplete_checks = {
        check.check_id: check.status.value
        for check in missing_gateway.to_live_validation_report().checks
    }
    assert incomplete_checks[SESSION_CONNECTORS_CHECK_ID] == "failed"

    duplicate_first_gateway = report.model_copy(
        update={
            "resources": [
                *report.resources,
                *[
                    resource.model_copy(update={"resource_id": f"duplicate-{resource.resource_id}"})
                    for resource in report.resources
                    if resource.kind in {"desktop_connector", "remote_connector"}
                ],
                CleanupResource(
                    kind="gateway_record",
                    resource_id="gateway-2",
                    location="desktop",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                ),
            ]
        }
    )
    duplicate_checks = {
        check.check_id: check.status.value
        for check in duplicate_first_gateway.to_live_validation_report().checks
    }
    assert duplicate_checks[SESSION_CONNECTORS_CHECK_ID] == "failed"


@pytest.mark.parametrize(
    ("outcome", "observed_state"),
    [("stopped", "inactive"), ("missing", "not-found")],
)
def test_worker_cleanup_requires_exact_terminal_post_stop_evidence(
    outcome: Literal["stopped", "missing"],
    observed_state: str,
) -> None:
    observed_at = datetime.now(UTC)
    report = SessionLifecycleReport(
        cluster="cluster",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=True,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="cluster",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="worker_service",
                resource_id="clio-relay-worker-cluster.service",
                location="cluster",
                action="stop",
                ownership_verified=True,
                outcome=outcome,
                verified_after_operation=True,
                observed_state=observed_state,
            ),
        ],
    )

    canonical = report.to_live_validation_report(stop_worker=True)
    worker_check = next(
        check for check in canonical.checks if check.check_id == SESSION_WORKER_CHECK_ID
    )

    assert worker_check.status.value == "passed"
    assert canonical.status.value == "passed"
    cleanup_payload = report.json_payload()["cleanup_evidence"]
    assert isinstance(cleanup_payload, dict)
    assert cleanup_payload["stop_worker"] is True


def test_teardown_remote_session_kills_owned_pid_and_optional_worker(
    monkeypatch: MonkeyPatch,
) -> None:
    scripts: list[str] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        del capture_output, check, timeout
        scripts.append(input.decode("utf-8"))
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "cluster": "ares",
                    "session_id": "session-1",
                    "mode": "teardown",
                    "cleanup_operation_id": "cleanup-test",
                    "cleanup_policy": {
                        "stop_worker": True,
                        "cancel_jobs": False,
                        "cancel_scheduler_jobs": False,
                    },
                    "relay_cancel_requested": False,
                    "scheduler_cancel_requested": False,
                    "resources": [
                        {
                            "kind": "remote_relay_api",
                            "resource_id": "123",
                            "location": "ares",
                            "action": "stop",
                            "ownership_verified": True,
                            "outcome": "stopped",
                            "residual": False,
                        },
                        {
                            "kind": "worker_service",
                            "resource_id": "clio-relay-worker-ares.service",
                            "location": "ares",
                            "action": "stop",
                            "ownership_verified": True,
                            "outcome": "stopped",
                            "residual": False,
                        },
                    ],
                    "errors": [],
                }
            ).encode("utf-8"),
            b"",
        )

    monkeypatch.setattr("clio_relay.session_lifecycle.subprocess.run", fake_run)

    report = teardown_remote_session(
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        expected_session_generation_id="generation-1",
        expected_cleanup_operation_id="cleanup-test",
        stop_worker=True,
        cluster="ares",
    )

    assert report.resources[0].outcome == "stopped"
    assert report.resources[1].resource_id == "clio-relay-worker-ares.service"
    assert report.to_cleanup_evidence(stop_worker=True).stop_worker is True
    assert "os.killpg" not in scripts[0]
    assert "process_start_ticks" in scripts[0]
    assert "ownership proof failed" in scripts[0]
    assert "token_group_processes" in scripts[0]
    token_scan = (
        scripts[0].split("def token_group_processes():", 1)[1].split("\npid = metadata.get", 1)[0]
    )
    assert "process_group ==" not in token_scan
    assert "proc.stat().st_uid != os.geteuid()" in token_scan
    assert 'export PATH="$HOME/.local/bin:$PATH"' in scripts[0]
    assert "observed_pgid = int(fields[2])" in token_scan
    assert "observed_pgid != recorded_pgid" in token_scan
    assert "cannot verify protected session process" in token_scan
    assert "os.pidfd_open(owned_pid, 0)" in scripts[0]
    assert "signal.pidfd_send_signal(process_fd, sig, None, 0)" in scripts[0]
    assert "signal_token_processes(signal.SIGTERM)" in scripts[0]
    assert "signal_token_processes(signal.SIGKILL)" in scripts[0]
    assert "flock -w 10 -x 9" in scripts[0]
    assert "timeout --signal=TERM --kill-after=5s 10s" in scripts[0]
    assert "timeout --signal=TERM --kill-after=5s 90s" in scripts[0]
    assert "owned session generation changed before teardown" in scripts[0]
    assert '--session-generation-id "$expected_session_generation_id"' in scripts[0]
    assert "--cleanup-stop-worker" in scripts[0]
    assert '["systemctl", "--user", "stop", service]' in scripts[0]
    assert 'active_state == "inactive"' in scripts[0]
    assert 'observed_state = "not-found" if service_missing else active_state' in scripts[0]
    assert '"verified_after_operation": verified_after_operation' in scripts[0]
    assert '"observed_state": observed_state' in scripts[0]
    assert "timeout=20" in scripts[0]
    assert "cleanup command timed out after 20 seconds" in scripts[0]

    with pytest.raises(RelayError, match="cleanup operation does not match"):
        teardown_remote_session(
            definition=ClusterDefinition(name="ares", ssh_host="ares"),
            session_id="session-1",
            expected_session_generation_id="generation-1",
            expected_cleanup_operation_id="cleanup-other",
            stop_worker=True,
            cluster="ares",
        )


def test_owned_teardown_revalidates_exact_pidfd_identity_after_leader_pid_reuse() -> None:
    script = session_lifecycle._owned_teardown_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        expected_session_generation_id="generation-1",
        stop_worker=False,
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
        cluster="ares",
    )

    pidfd_open = script.index("process_fd = os.pidfd_open(owned_pid, 0)")
    identity_recheck = script.index("if owned_pid not in token_group_processes():")
    pidfd_signal = script.index("signal.pidfd_send_signal(process_fd, sig, None, 0)")
    assert pidfd_open < identity_recheck < pidfd_signal
    assert "os.killpg" not in script
    assert "running = bool(owned_group_pids)" in script
    assert "post_running = bool(token_group_processes())" in script
    teardown_program = script.split("<<'__CLIO_RELAY_OWNED_TEARDOWN__'\n", 1)[1].split(
        "\n__CLIO_RELAY_OWNED_TEARDOWN__",
        1,
    )[0]
    compile(teardown_program, "owned-session-teardown", "exec")

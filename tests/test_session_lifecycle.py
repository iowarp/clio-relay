from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from typing import Literal

import pytest
from pytest import MonkeyPatch

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.session_lifecycle import (
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


def test_start_remote_session_writes_owned_pid_and_metadata(monkeypatch: MonkeyPatch) -> None:
    calls: list[tuple[list[str], str]] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        calls.append((command, input.decode("utf-8")))
        assert capture_output is True
        assert check is False
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
    assert "umask 077" in script
    assert "trap cleanup_incomplete_start EXIT" in script
    assert "flock -x 9" in script
    assert "CLIO_RELAY_SESSION_GENERATION_ID" in script
    assert "session_generation_id" in script
    assert "clio-relay session prepare-start" in script
    assert '--recorded-generation-id "$recorded_generation_id"' in script
    assert script.index("clio-relay session prepare-start") < script.index(
        'kill -- "-$existing_owned_pgid"'
    )
    assert "clio-relay session resume-intake" in script
    assert '--session-generation-id "$session_generation_id"' in script
    assert script.index("flock -x 9") < script.index("clio-relay session resume-intake")
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
    ) -> subprocess.CompletedProcess[bytes]:
        del capture_output, check
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
    ) -> subprocess.CompletedProcess[bytes]:
        del capture_output, check
        scripts.append(input.decode("utf-8"))
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "cluster": "ares",
                    "session_id": "session-1",
                    "mode": "teardown",
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
        stop_worker=True,
        cluster="ares",
    )

    assert report.resources[0].outcome == "stopped"
    assert report.resources[1].resource_id == "clio-relay-worker-ares.service"
    assert report.to_cleanup_evidence(stop_worker=True).stop_worker is True
    assert "os.killpg" in scripts[0]
    assert "process_start_ticks" in scripts[0]
    assert "ownership proof failed" in scripts[0]
    assert "token_group_processes" in scripts[0]
    assert "flock -x 9" in scripts[0]
    assert "owned session generation changed before teardown" in scripts[0]
    assert '--session-generation-id "$expected_session_generation_id"' in scripts[0]
    assert '["systemctl", "--user", "stop", service]' in scripts[0]
    assert 'active_state == "inactive"' in scripts[0]
    assert 'observed_state = "not-found" if service_missing else active_state' in scripts[0]
    assert '"verified_after_operation": verified_after_operation' in scripts[0]
    assert '"observed_state": observed_state' in scripts[0]

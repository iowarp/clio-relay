"""Focused contracts for persistent endpoint worker recovery."""

from __future__ import annotations

import json
import subprocess
from typing import cast

import pytest
from typer.testing import CliRunner

import clio_relay.cli as cli
import clio_relay.endpoint_service_status as service_status
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.deployment import render_endpoint_user_service
from clio_relay.endpoint_service_status import (
    EndpointServiceReadiness,
    endpoint_service_readiness_over_ssh,
    parse_endpoint_service_readiness,
    render_endpoint_service_readiness_script,
)


def _status_output(**overrides: str) -> str:
    values = {
        "LoadState": "loaded",
        "UnitFileState": "enabled",
        "ActiveState": "active",
        "SubState": "running",
        "Result": "success",
        "InvocationID": "1" * 32,
        "Restart": "always",
        "RestartUSec": "5s",
        "NRestarts": "2",
        "StartLimitIntervalUSec": "0",
        "StartLimitBurst": "5",
        "Linger": "yes",
        "ActivationJobId": "none",
        "ActivationJobType": "none",
        "ActivationJobState": "none",
    }
    values.update(overrides)
    return "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"


def test_endpoint_unit_recovers_without_restart_rate_exhaustion() -> None:
    """Unexpected exits remain paced and cannot strand the enabled worker."""
    unit = render_endpoint_user_service(
        cluster="alpha",
        definition=ClusterDefinition(name="alpha", ssh_host="alpha-login"),
    )

    assert "StartLimitIntervalSec=0" in unit
    assert "Restart=always" in unit
    assert "RestartSec=5" in unit
    assert "TimeoutStartSec=300s" in unit
    assert "ExecStop=" not in unit
    assert "scancel" not in unit
    assert "queue cancel" not in unit


def test_endpoint_service_status_probe_is_bounded_and_read_only() -> None:
    """Diagnosis cannot start, stop, dequeue, or cancel application work."""
    script = render_endpoint_service_readiness_script(
        service_name="clio-relay-worker-alpha.service"
    )

    assert script.count("timeout --signal=TERM --kill-after=2s 5s") == 3
    assert "systemctl --user show" in script
    assert "systemctl --user list-jobs" in script
    assert "systemctl --user start" not in script
    assert "systemctl --user restart" not in script
    assert "systemctl --user stop" not in script
    assert "scancel" not in script
    assert "queue cancel" not in script


def test_ready_service_reports_restart_and_logout_persistence() -> None:
    evidence = parse_endpoint_service_readiness(
        cluster="alpha",
        service_name="clio-relay-worker-alpha.service",
        output=_status_output(),
    )

    assert evidence.schema_version == "clio-relay.endpoint-service-readiness.v1"
    assert evidence.ready is True
    assert evidence.recovery_state == "ready"
    assert evidence.self_healing_configured is True
    assert evidence.persistent_across_logout is True
    assert evidence.persistence == "systemd-user-linger"
    assert evidence.automatic_restart_count == 2
    assert evidence.service_restart_preserves_durable_queue is True
    assert evidence.service_restart_cancels_scheduler_jobs is False


def test_intentional_stop_is_distinct_from_failed_worker_exit() -> None:
    intentional = parse_endpoint_service_readiness(
        cluster="alpha",
        service_name="clio-relay-worker-alpha.service",
        output=_status_output(
            ActiveState="inactive",
            SubState="dead",
            Result="success",
            InvocationID="",
        ),
    )
    failed = parse_endpoint_service_readiness(
        cluster="alpha",
        service_name="clio-relay-worker-alpha.service",
        output=_status_output(
            ActiveState="failed",
            SubState="failed",
            Result="exit-code",
            InvocationID="",
        ),
    )

    assert intentional.ready is False
    assert intentional.intentional_stop is True
    assert intentional.recovery_state == "intentional-stop"
    assert intentional.operator_action == (
        "clio-relay cluster restart-endpoint-service --cluster alpha"
    )
    assert failed.ready is False
    assert failed.intentional_stop is False
    assert failed.recovery_state == "failed"
    assert failed.operator_action == (
        "journalctl --user --unit=clio-relay-worker-alpha.service --lines=50 --no-pager"
    )


def test_automatic_restart_is_reported_as_recovering_not_inactive() -> None:
    evidence = parse_endpoint_service_readiness(
        cluster="alpha",
        service_name="clio-relay-worker-alpha.service",
        output=_status_output(
            ActiveState="activating",
            SubState="auto-restart",
            Result="exit-code",
            InvocationID="",
            NRestarts="3",
            ActivationJobId="417",
            ActivationJobType="start",
            ActivationJobState="waiting",
        ),
    )

    assert evidence.ready is False
    assert evidence.activation_pending is True
    assert evidence.recovery_state == "recovering"
    assert evidence.automatic_restart_count == 3
    assert evidence.activation_job_id == "417"


@pytest.mark.parametrize(
    ("overrides", "diagnosis"),
    [
        (
            {"Restart": "on-failure", "StartLimitIntervalUSec": "10s"},
            "does not have persistent crash recovery",
        ),
        ({"Linger": "no"}, "login-scoped"),
    ],
)
def test_readiness_rejects_nonpersistent_service_policy(
    overrides: dict[str, str],
    diagnosis: str,
) -> None:
    evidence = parse_endpoint_service_readiness(
        cluster="alpha",
        service_name="clio-relay-worker-alpha.service",
        output=_status_output(**overrides),
    )

    assert evidence.ready is False
    assert evidence.recovery_state == "degraded"
    assert diagnosis in evidence.diagnosis


def test_remote_status_call_returns_unhealthy_evidence_without_mutation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        observed.update(
            command=command,
            script=input.decode("utf-8"),
            capture_output=capture_output,
            check=check,
            timeout=timeout,
        )
        return subprocess.CompletedProcess(
            command,
            0,
            _status_output(
                ActiveState="inactive",
                SubState="dead",
                Result="success",
                InvocationID="",
            ).encode("utf-8"),
            b"",
        )

    monkeypatch.setattr(service_status.subprocess, "run", run)

    evidence = endpoint_service_readiness_over_ssh(
        cluster="alpha",
        ssh_host="alpha-login",
        timeout_seconds=11,
    )

    assert evidence.recovery_state == "intentional-stop"
    assert observed["command"] == ["ssh", "alpha-login", "bash", "-s"]
    assert observed["timeout"] == 11
    script = cast(str, observed["script"])
    assert "systemctl --user show" in script
    assert "systemctl --user start" not in script


def test_cluster_endpoint_service_status_prints_machine_readable_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = parse_endpoint_service_readiness(
        cluster="alpha",
        service_name="clio-relay-worker-alpha.service",
        output=_status_output(),
    )
    observed: dict[str, str] = {}

    def require_cluster(cluster: str) -> ClusterDefinition:
        return ClusterDefinition(name=cluster, ssh_host="alpha-login")

    monkeypatch.setattr(cli, "_require_cluster", require_cluster)

    def inspect(*, cluster: str, ssh_host: str) -> EndpointServiceReadiness:
        observed.update(cluster=cluster, ssh_host=ssh_host)
        return evidence

    monkeypatch.setattr(cli, "endpoint_service_readiness_over_ssh", inspect)

    result = CliRunner().invoke(
        app,
        ["cluster", "endpoint-service-status", "--cluster", "alpha"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == "clio-relay.endpoint-service-readiness.v1"
    assert payload["ready"] is True
    assert payload["service_restart_preserves_durable_queue"] is True
    assert payload["service_restart_cancels_scheduler_jobs"] is False
    assert observed == {"cluster": "alpha", "ssh_host": "alpha-login"}

"""Acceptance-capable CLI commands persist canonical reports by default."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from clio_relay import cli
from clio_relay.cli import app
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    FrpTransportConfig,
)
from clio_relay.errors import RelayError
from clio_relay.validation_report import load_validation_report


@pytest.fixture(autouse=True)
def _local_cli(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")


def _write_cluster(root: Path, name: str = "test-cluster") -> None:
    ClusterRegistry(
        clusters={
            name: ClusterDefinition(
                name=name,
                ssh_host=name,
                frp_transport=FrpTransportConfig(server_addr="relay.example.test"),
            )
        }
    ).save(root / ".clio-relay" / "clusters.json")


def _report_paths(root: Path) -> list[Path]:
    return sorted((root / ".clio-relay" / "validation-reports").glob("*.json"))


def test_frpc_connection_writes_distinct_default_reports_on_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")

    def successful_probe(**_kwargs: object) -> list[str]:
        return ["frpc stayed connected until timeout", "login to server success"]

    monkeypatch.setattr(cli, "run_frpc_connection_check", successful_probe)
    command = [
        "relay-host",
        "test-frpc-connection",
        "--cluster",
        "test-cluster",
        "--local-port",
        "8848",
        "--validation-launcher",
        "uvx",
        "--validation-install-source",
        "wheel:clio_relay-1.0.0-py3-none-any.whl",
    ]

    first = CliRunner().invoke(app, command)
    second = CliRunner().invoke(app, command)

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    reports = _report_paths(tmp_path)
    assert len(reports) == 2
    assert reports[0] != reports[1]
    parsed = [load_validation_report(path) for path in reports]
    assert len({report.report_id for report in parsed}) == 2
    for report in parsed:
        assert report.status.value == "passed"
        assert [check.check_id for check in report.checks] == ["transport.frpc-connection"]
        assert report.resources[0].state == "stopped"
        assert report.resources[0].metadata["cleanup_verified"] is True
        assert report.cleanup.remaining_resources == []
    assert "validation.report=" in first.output


def test_frpc_connection_writes_default_failure_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")

    def failed_probe(**_kwargs: object) -> list[str]:
        raise RelayError("frpc exited before the connection interval")

    monkeypatch.setattr(cli, "run_frpc_connection_check", failed_probe)

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-frpc-connection",
            "--cluster",
            "test-cluster",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 1
    reports = _report_paths(tmp_path)
    assert len(reports) == 1
    report = load_validation_report(reports[0])
    assert report.status.value == "failed"
    assert report.checks[-1].check_id == "transport.frpc-connection"
    assert report.checks[-1].status.value == "failed"
    assert report.cleanup.remaining_resources == []


@pytest.mark.parametrize(
    "command",
    [
        ["session", "detach", "--cluster", "missing", "--session-id", "owned"],
        ["session", "teardown", "--cluster", "missing", "--session-id", "owned"],
        [
            "gateway",
            "start-runtime",
            "--cluster",
            "missing",
            "--name",
            "runtime",
            "--runtime-json-file",
            "missing-runtime.json",
        ],
        ["gateway", "detach-runtime", "owned", "--cluster", "missing"],
        ["gateway", "stop-runtime", "owned", "--cluster", "missing"],
        ["cluster", "bootstrap", "--cluster", "missing"],
    ],
)
def test_acceptance_commands_write_default_preflight_failure_reports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    ClusterRegistry.default().save(tmp_path / ".clio-relay" / "clusters.json")

    result = CliRunner().invoke(app, command)

    assert result.exit_code != 0
    reports = _report_paths(tmp_path)
    assert len(reports) == 1
    payload = json.loads(reports[0].read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["completed_at"] is not None
    assert payload["checks"][-1]["status"] == "failed"

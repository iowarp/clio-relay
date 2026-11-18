"""Acceptance-capable CLI commands persist canonical reports by default."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

import clio_relay.frp_check as frp_check
from clio_relay.adapters.worker.cli import worker_app as app
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    FrpTransportConfig,
    RemoteMcpServerConfig,
)
from clio_relay.errors import RelayError
from clio_relay.validation_report import (
    load_validation_report,
)


@pytest.fixture(autouse=True)
def _local_cli(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Pin the CLI mode and the cleanup-evidence lock directory (clio-relay#289).

    ``session teardown``/``session detach`` acquire a crash-released flock on
    ``<install-receipt-parent>/.clio-cleanup-evidence-v1.lock`` (see
    ``cli_cleanup_evidence._acquire_cleanup_evidence_lock``). That parent is
    ``default_install_receipt_path().parent`` -- ``Path.home() / ".local" /
    "share" / "clio-relay"`` by default -- which is keyed on the user's home
    directory, NOT the process cwd, so ``monkeypatch.chdir(tmp_path)`` alone
    never isolates it: every test process on this machine, and any real
    concurrently-running ``clio-relay worker session teardown``, contend on the
    exact same lock file regardless of cwd (live-proven evidence: a real
    ares recovery teardown collided with this suite's own session-teardown
    tests, both racing for that one shared home-scoped lock, producing
    ``RelayError('another cleanup is writing evidence in this directory')``).
    Pinning ``CLIO_RELAY_INSTALL_RECEIPT`` into ``tmp_path`` -- the existing
    seam ``default_install_receipt_path`` already reads -- gives every test
    its own private lock directory, matching the established convention in
    ``tests/test_cli.py``/``tests/test_cli_session.py`` (and every other
    ``test_cli_*.py`` sibling) that this file was missing.
    """
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def _write_cluster(root: Path, name: str = "test-cluster") -> None:
    ClusterRegistry(
        clusters={
            name: ClusterDefinition(
                name=name,
                ssh_host=name,
                frp_transport=FrpTransportConfig(server_addr="relay.example.test"),
                remote_mcp_servers={
                    "science": RemoteMcpServerConfig(
                        command="science-mcp",
                        allow_tools=["inspect"],
                        profiles=["user"],
                    )
                },
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

    monkeypatch.setattr(frp_check, "run_frpc_connection_check", successful_probe)
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

    monkeypatch.setattr(frp_check, "run_frpc_connection_check", failed_probe)

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

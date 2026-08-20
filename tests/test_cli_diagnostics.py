"""Tests for the ``doctor``/``live-test`` top-level command group
(iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` alongside the two commands'
extraction into ``src/clio_relay/cli_diagnostics.py``, per ground rule 3
(SS2 of ``docs/design/relay-architecture-2026-08.md``): a test reachable
only through this command group moves with the logic it exercises.

``doctor`` itself had no dedicated ``CliRunner``-level test in
``tests/test_cli.py`` -- its underlying logic (``run_doctor``/
``run_cluster_doctor``) is covered by ``tests/test_doctor_and_relay_host.py``
-- so only the two ``live-test`` tests move here. The shared, parametrized
``test_cli_acceptance_preflight_failure_always_writes_canonical_report``
stays in ``tests/test_cli.py``: it exercises ``live-test`` as one case among
several unrelated command groups (``relay-host``, ``session detach``,
``session teardown``), the same "shared parametrized test" precedent
``tests/test_cli_cluster_deploy.py``'s own docstring and R8(ii)'s
``relay-host`` extraction both name.

**Patch-target parity.** ``test_live_test_resume_uses_sibling_report_and_
preserves_checkpoint`` patches ``live_acceptance.run_live_acceptance`` on the
owner module directly (not through ``cli.py``), so the move needed no
patch-target change at all.

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on, plus a session-teardown collaborator half
neither test below exercises). Reproduced here as the env-var half only, the
same precedent ``tests/test_cli_cluster_deploy.py``'s own copy established.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.live_acceptance as live_acceptance
from clio_relay.cli import app
from clio_relay.live_acceptance import LiveAcceptanceOptions
from clio_relay.validation_report import (
    LiveValidationReport,
    ValidationStatus,
    new_live_validation_report,
    write_validation_report,
)
from tests.test_cli import (
    _write_passing_validation_report,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
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


def test_live_test_replaces_stale_success_when_secret_resolution_fails(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    report_path = tmp_path / "live-test.json"
    stale_report_id = _write_passing_validation_report(
        report_path,
        scenario="live-test",
        cluster="ares",
    )

    result = CliRunner().invoke(
        app,
        [
            "live-test",
            "--cluster",
            "ares",
            "--verify-transport",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    current = LiveValidationReport.model_validate_json(report_path.read_text(encoding="utf-8"))
    assert current.report_id != stale_report_id
    assert current.scenario == "live-test"
    assert current.cluster == "ares"
    assert current.status.value == "failed"
    assert current.checks[-1].check_id == "live.completed"
    assert "frp token" in (current.error or "")


def test_live_test_resume_uses_sibling_report_and_preserves_checkpoint(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The CLI never seeds or overwrites the PENDING report selected for resume."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    source_path = tmp_path / "pending-live-test.json"
    source = new_live_validation_report(scenario="live-test", cluster="ares")
    source.status = ValidationStatus.PENDING
    source.completed_at = datetime.now(UTC)
    write_validation_report(source, source_path)
    source_bytes = source_path.read_bytes()
    captured: list[LiveAcceptanceOptions] = []

    def fake_run(options: LiveAcceptanceOptions) -> list[str]:
        captured.append(options)
        report_path = options.report_path
        assert report_path is not None
        report_id = options.report_id
        report = new_live_validation_report(
            scenario="live-test",
            cluster="ares",
            report_id=report_id,
        )
        report.status = ValidationStatus.PENDING
        report.completed_at = datetime.now(UTC)
        write_validation_report(report, report_path)
        return ["validation.status=pending", f"validation.report={report_path.resolve()}"]

    monkeypatch.setattr(live_acceptance, "run_live_acceptance", fake_run)
    result = CliRunner().invoke(
        app,
        [
            "live-test",
            "--cluster",
            "ares",
            "--jarvis-yaml",
            str(pipeline),
            "--resume-report",
            str(source_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert source_path.read_bytes() == source_bytes
    assert len(captured) == 1
    options = captured[0]
    assert options.resume_report_path == source_path
    output_path = options.report_path
    assert output_path is not None
    assert output_path != source_path
    assert output_path.exists()
    assert output_path.parent == source_path.parent

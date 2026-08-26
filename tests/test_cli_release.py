"""Tests for the ``release`` command group (iowarp/clio-relay#231).

This moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the ``release_app`` commands' extraction into
``src/clio_relay/cli_release.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises. No dedicated
test existed for ``release gate``/``release preflight`` in
``tests/test_cli.py`` -- only ``release validate-local``, moved below.

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on). The test below never reaches
cluster-passthrough logic, so it does not strictly need the default, but the
fixture is reproduced anyway (the env-var half only, the same precedent
``tests/test_cli_relay_host.py``'s own ``_default_cli_mode`` established)
so a future test added here does not silently lose it -- the trap
``tests/test_cli_worker.py``'s docstring documents hitting for real.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.release_validation as release_validation
from clio_relay.cli import app
from clio_relay.release_validation import LocalReleaseValidationOptions
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    new_live_validation_report,
    write_validation_report,
)
from tests.test_cli import _write_passing_validation_report


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


def test_release_validate_local_replaces_stale_success_on_preflight_failure(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "local-release.json"
    stale_report_id = _write_passing_validation_report(
        report_path,
        scenario="local-release",
        cluster="local",
    )

    result = CliRunner().invoke(
        app,
        [
            "release",
            "validate-local",
            "--project-root",
            str(tmp_path / "missing-checkout"),
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    current = LiveValidationReport.model_validate_json(report_path.read_text(encoding="utf-8"))
    assert current.report_id != stale_report_id
    assert current.scenario == "local-release"
    assert current.cluster == "local"
    assert current.status.value == "failed"
    assert current.checks[-1].check_id == "local-release.completed"
    assert "has no pyproject.toml" in (current.error or "")


def test_release_validate_local_rejects_negative_check_timeout_before_seeding(
    tmp_path: Path,
) -> None:
    """clio-relay#275 review D5: a negative --check-timeout-seconds is refused
    by typer's own min=0 at the parsing boundary, before any report exists."""
    report_path = tmp_path / "local-release.json"

    result = CliRunner().invoke(
        app,
        [
            "release",
            "validate-local",
            "--check-timeout-seconds",
            "-5",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 2
    assert not report_path.exists()


def test_release_validate_local_rejects_non_positive_pytest_timeout_before_seeding(
    tmp_path: Path,
) -> None:
    """clio-relay#275 review D5: pytest-timeout treats a non-positive value as
    no bound, silently defeating the option -- refused explicitly instead,
    before any report is seeded."""
    report_path = tmp_path / "local-release.json"

    result = CliRunner().invoke(
        app,
        [
            "release",
            "validate-local",
            "--pytest-per-test-timeout-seconds",
            "0",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    assert "must be > 0" in result.output
    assert not report_path.exists()


def test_release_validate_local_maps_zero_check_timeout_to_unbounded(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """clio-relay#275 review D5: 0 is the documented CLI spelling for "no
    bound", translated to the internal None (the only value that actually
    reaches an unbounded ``run_streaming_command`` call)."""
    captured: list[LocalReleaseValidationOptions] = []

    def fake_run(
        options: LocalReleaseValidationOptions,
        *,
        runner: object = None,
    ) -> LiveValidationReport:
        del runner
        captured.append(options)
        report = new_live_validation_report(
            scenario="local-release",
            cluster="local",
            report_id=options.report_id,
        )
        recorder = ValidationRecorder(report)
        with recorder.check("acceptance.completed", "complete acceptance command") as evidence:
            evidence.append(EvidenceReference(kind="test", excerpt="acceptance completed"))
        recorder.finish()
        write_validation_report(report, options.report_path)
        return report

    monkeypatch.setattr(release_validation, "run_local_release_validation", fake_run)
    report_path = tmp_path / "local-release.json"

    result = CliRunner().invoke(
        app,
        ["release", "validate-local", "--check-timeout-seconds", "0", "--report", str(report_path)],
    )

    assert result.exit_code == 0, result.output
    assert len(captured) == 1
    assert captured[0].check_timeout_seconds is None

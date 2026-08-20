"""Tests for the ``queue`` maintenance command group (iowarp/clio-relay#231).

This moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the ``queue validate`` command's extraction into
``src/clio_relay/cli_queue_maintenance.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises. No dedicated
test existed for retention-plan/retention-status/retention-collect in
``tests/test_cli.py``; ``cleanup-stale`` is covered as one step of
``test_cli_queue_management_commands`` in ``tests/test_cli_queue.py``
(that group's own docstring explains why it stays there).

``monkeypatch.setattr(scheduler_providers, "validation_provider_for_scheduler", ...)``
already patched the real owner module directly (the R8(i) idiom, unaffected
by which file calls it).

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on). It is reproduced here (the env-var half only,
the same precedent ``tests/test_cli_relay_host.py``'s own
``_default_cli_mode`` established) -- the trap
``tests/test_cli_worker.py``'s docstring documents hitting for real.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.scheduler_providers as scheduler_providers
from clio_relay.cli import app
from tests.queue_validation_fixtures import (
    DeterministicQueueValidationProvider,
    LiveWorkerFleet,
)
from tests.test_cli import _write_test_cluster


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


def test_cli_queue_validation_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, "test-cluster", scheduler_provider="slurm")
    fleet = LiveWorkerFleet(tmp_path).start()
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(fleet.settings.core_dir))
    report_path = tmp_path / "queue-validation.json"

    def queue_validation_provider(_name: str | None) -> DeterministicQueueValidationProvider:
        return fleet.scheduler

    monkeypatch.setattr(
        scheduler_providers, "validation_provider_for_scheduler", queue_validation_provider
    )
    try:
        result = CliRunner().invoke(
            app,
            [
                "queue",
                "validate",
                "--cluster",
                "test-cluster",
                "--older-than",
                "1s",
                "--scheduler-timeout-seconds",
                "30",
                "--scheduler-poll-seconds",
                "0.02",
                "--report",
                str(report_path),
            ],
        )
    finally:
        fleet.close()

    failure_report = (
        report_path.read_text(encoding="utf-8") if report_path.exists() else "<report not written>"
    )
    assert result.exit_code == 0, (
        f"output={result.output!r}\nexception={result.exception!r}\nreport={failure_report}"
    )
    assert "validation.status=passed" in result.output
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["scenario"] == "queue-management"
    assert {check["check_id"] for check in report["checks"]} == {
        "queue.kind-concurrency-parallel",
        "queue.kind-concurrency-worker-enforced",
        "queue.lease-capacity-audit-initial",
        "queue.lease-capacity-audit-final",
        "queue.list-bounded",
        "queue.diagnose-specific-reason",
        "queue.stale-dry-run",
        "queue.stale-cleanup-executed",
        "queue.cancel-running-worker-process",
        "queue.scheduler-preserved-default",
        "queue.worker-containment-enforced",
    }
    assert report["cleanup"]["cancel_scheduler_jobs"] is False

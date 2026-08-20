"""Tests for the ``scheduler`` command group (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the ``scheduler_app`` commands' extraction into
``src/clio_relay/cli_scheduler.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.
``test_scheduler_phase_batch_uses_one_remote_command`` stays in
``tests/test_cli.py`` -- it calls ``cli._scheduler_phases_after_operation``
directly, a separate cli.py-local helper used by session teardown, not one
of this group's eleven commands.

``monkeypatch.setattr(scheduler_providers, "provider_for_scheduler", ...)``
already patched the real owner module directly (the R8(i) idiom, unaffected
by which file calls it), so no patch-target re-pointing was needed here.

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
from types import SimpleNamespace
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.scheduler_providers as scheduler_providers
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterRegistry
from clio_relay.models import SchedulerPhase, SchedulerStatus
from tests.test_cli import (
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


def test_cli_scheduler_preflight_failure_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    ClusterRegistry.default().save(tmp_path / ".clio-relay" / "clusters.json")
    report_path = tmp_path / "scheduler-preflight-failed.json"

    result = CliRunner().invoke(
        app,
        [
            "scheduler",
            "validate-lifecycle",
            "--cluster",
            "missing",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code != 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "scheduler.preflight"


def test_scheduler_status_batch_command_returns_each_exact_identity(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, name="configured-target", scheduler_provider="slurm")

    def poll(scheduler_job_id: str) -> SchedulerStatus:
        return SchedulerStatus(
            scheduler="slurm",
            scheduler_job_id=scheduler_job_id,
            phase=(
                SchedulerPhase.COMPLETED if scheduler_job_id == "101" else SchedulerPhase.RUNNING
            ),
            record_found=True,
            active_record_found=scheduler_job_id != "101",
        )

    scheduler = SimpleNamespace(poll=poll)

    def resolve_provider(_provider: str) -> Any:
        return scheduler

    monkeypatch.setattr(scheduler_providers, "provider_for_scheduler", resolve_provider)

    result = CliRunner().invoke(
        app,
        [
            "scheduler",
            "status-batch",
            "--cluster",
            "configured-target",
            "--provider",
            "slurm",
            "--scheduler-job-id",
            "101",
            "--scheduler-job-id",
            "102",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == "clio-relay.scheduler-status-batch.v1"
    assert [status["scheduler_job_id"] for status in payload["statuses"]] == [
        "101",
        "102",
    ]

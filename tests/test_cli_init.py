"""Tests for the ``init``/``install-frp`` top-level command group
(iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` alongside the two commands'
extraction into ``src/clio_relay/cli_init.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.

``install-frp`` had no dedicated test anywhere in the suite (a thin wrapper
around ``bootstrap.install_local_frp``, itself untouched by this slice), so
only the two ``init`` tests move here.

**Patch-target parity.** Both tests patch ``storage_runtime.
storage_managed_queue`` on the owner module directly (not through
``cli.py``), so the move needed no patch-target change at all.

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on, plus a session-teardown collaborator half
neither test below exercises). Reproduced here as the env-var half only, the
same precedent ``tests/test_cli_cluster_deploy.py``/``tests/test_cli_
diagnostics.py`` each already established.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.storage_runtime as storage_runtime
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterRegistry


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


def test_cli_init_creates_empty_cluster_registry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))

    result = CliRunner().invoke(app, ["init"])

    assert result.exit_code == 0
    assert "clusters=" in result.output
    assert "ares" not in result.output
    registry = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json")
    assert registry.clusters == {}


def test_cli_init_threads_explicit_legacy_output_migration_authorization(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    observed: list[bool] = []

    def capture_authorization(
        _settings: object,
        *,
        migrate_legacy_output: bool = False,
    ) -> object:
        observed.append(migrate_legacy_output)
        return object()

    monkeypatch.setattr(storage_runtime, "storage_managed_queue", capture_authorization)

    result = CliRunner().invoke(app, ["init", "--migrate-legacy-output"])

    assert result.exit_code == 0
    assert observed == [True]

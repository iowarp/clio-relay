"""Tests for the ``worker`` command group (iowarp/clio-relay#231).

This moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the ``worker_app`` command's extraction into
``src/clio_relay/cli_worker.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.

**Known-trap autouse-fixture fix.** ``test_cli.py`` defines its own
module-scoped ``autouse=True`` ``_default_cli_mode`` fixture (env-var
defaults every CLI invocation there relies on, notably
``CLIO_RELAY_CLI_MODE=local`` so cluster-passthrough commands do not try to
resolve a real registered cluster). Autouse fixtures are file-scoped unless
promoted to a ``conftest.py``, so a test moved verbatim into its own file
silently loses that default. This file mirrors the env-var half only, the
same precedent ``tests/test_cli_relay_host.py``'s own ``_default_cli_mode``
established (its docstring explains the split in full).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import EndpointRegistration, EndpointRole


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


def test_cli_worker_status_reports_registered_capacity(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="test-cluster",
            hostname="node",
            pid=123,
            metadata={"concurrency": 3},
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["worker", "status", "--cluster", "test-cluster"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["worker_count"] == 1
    assert payload["configured_concurrency"] == 3

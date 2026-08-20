"""Tests for the ``remote-mcp`` command group's exclusive helpers
(iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` alongside ``_read_remote_mcp_
result_artifact``/``_read_local_mcp_result_artifact``'s extraction into
``src/clio_relay/cli_remote_mcp.py`` (that module's own docstring names
these as "exclusive helpers moved with their only caller"). Neither test
drives ``CliRunner``; both call the moved function directly to prove the
shared duplicate-artifact ambiguity guard (``cli._artifact_record``, which
stays cli.py-resident) still fires through the relocated wrapper. The
``monkeypatch.setattr(cli, "_remote_artifact_records", ...)``/
``monkeypatch.setattr(cli, "_complete_local_artifact_records", ...)`` calls
below are unchanged from the original -- those two collaborators stay
cli.py-resident (the JARVIS execution-query engine still calls them), and
``cli_remote_mcp.py`` reaches them via a function-local
``import clio_relay.cli as cli`` module-attribute lookup, so patching the
``cli`` module directly is still the correct seam.

The bulk of ``remote-mcp`` command coverage (register/unregister/list/
reload/refresh/validate) lives in ``tests/test_remote_mcp.py`` (a different
split's file); it was audited and its own monkeypatch targets updated
in-place for this extraction rather than moved here, since it isn't part of
the ``test_cli.py`` family this campaign is decomposing.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

import clio_relay.cli as cli
import clio_relay.cli_remote_mcp as cli_remote_mcp
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError


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


def test_jarvis_discovery_rejects_ambiguous_mcp_result_artifacts(
    monkeypatch: MonkeyPatch,
) -> None:
    """A retry cannot make an earlier MCP result the implicit discovery authority."""
    definition = ClusterDefinition(name="configured-target", ssh_host="cluster.example")

    def duplicate_results(
        _definition: ClusterDefinition,
        _job_id: str,
    ) -> list[dict[str, object]]:
        return [
            {"artifact_id": "artifact-first", "kind": "mcp_result"},
            {"artifact_id": "artifact-retry", "kind": "mcp_result"},
        ]

    monkeypatch.setattr(cli, "_remote_artifact_records", duplicate_results)

    with pytest.raises(RelayError, match="durable artifact authority is ambiguous"):
        cli_remote_mcp._read_remote_mcp_result_artifact(  # noqa: SLF001
            definition,
            "job-retried-discovery",
        )


def test_local_jarvis_discovery_rejects_ambiguous_mcp_result_artifacts(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Local mode applies the same unique durable authority as remote mode."""
    queue = ClioCoreQueue(tmp_path / "core")

    def duplicate_results(
        _queue: ClioCoreQueue,
        _job_id: str,
    ) -> list[dict[str, object]]:
        return [
            {"artifact_id": "artifact-first", "kind": "mcp_result"},
            {"artifact_id": "artifact-retry", "kind": "mcp_result"},
        ]

    monkeypatch.setattr(cli, "_complete_local_artifact_records", duplicate_results)

    with pytest.raises(RelayError, match="durable artifact authority is ambiguous"):
        cli_remote_mcp._read_local_mcp_result_artifact(  # noqa: SLF001
            queue,
            "job-retried-local-discovery",
        )

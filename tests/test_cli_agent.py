"""Tests for the ``agent`` command group (iowarp/clio-relay#231).

This moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the ``agent_app`` commands' extraction into
``src/clio_relay/cli_agent.py``, per ground rule 3 (§2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on). The test below overrides ``CLIO_RELAY_CLI_MODE``
to ``"ssh"`` itself, so it does not strictly need the default, but the
fixture is reproduced anyway (the env-var half only, the same precedent
``tests/test_cli_relay_host.py``'s own ``_default_cli_mode`` established)
so a future test added here does not silently lose it -- the trap
``tests/test_cli_worker.py``'s docstring documents hitting for real.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from clio_relay.cli import app
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


def test_cli_remote_agent_run_preserves_posix_prompt_path(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b"job_agent\n", b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "agent",
            "run",
            "--cluster",
            "ares",
            "--prompt",
            "/home/user/prompt.md",
            "--mcp-config",
            "/home/user/mcp.toml",
            "--idempotency-key",
            "agent-posix-path",
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "job_agent"
    assert "--prompt /home/user/prompt.md" in commands[0][2]
    assert "--mcp-config /home/user/mcp.toml" in commands[0][2]
    assert "\\home\\user" not in commands[0][2]

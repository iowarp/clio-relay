"""Tests for the internal ``session ...-owned``/intake commands
(iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond import and
patch-target updates) alongside their commands' extraction into
``src/clio_relay/cli_session_owned.py``, per ground rule 3 (§2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through one of the moved commands, or through
``_inspect_owned_session_recovery_before_start`` (moved alongside
``recovery-status``, see ``cli_session_owned.py``'s own docstring), moves
with the logic it exercises. The four ``test_owned_session_recovery_*``
tests that call ``cli._inspect_owned_session_recovery_after_transition``
directly stay in ``test_cli.py`` -- that sibling function has a second call
site outside ``session_app`` entirely and stays in ``cli.py`` as a shared
collaborator.

Both ``test_owned_session_pre_start_probe_*`` tests call
``cli_session_owned._inspect_owned_session_recovery_before_start`` directly
(the moved function) with an explicit ``timeout_seconds``, so the default
-value adaptation that module's docstring describes (``None`` resolved to
``cli.OWNED_SESSION_RECOVERY_TRANSITION_TIMEOUT_SECONDS`` inside the
function body, instead of at the ``def`` site) is never exercised by either
test -- both always pass their own explicit value.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.cli_owned_session_recovery as cli_owned_session_recovery
import clio_relay.cli_session_owned as cli_session_owned
from clio_relay.cli import app
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import GatewaySession, JarvisRunSpec, JobKind, RelayJob
from clio_relay.session_lifecycle import OwnedSessionRecoveryStatus
from tests.test_cli import (
    _activate_owner_session,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only.

    That fixture also monkeypatches ``cli._persist_verified_cleanup_report_
    before_closure``/``cli._owned_session_recovery_status`` for session-
    teardown tests; none of the tests in this file exercise that path, so
    only the two environment variables every CLI invocation here relies on
    (local mode, a real install-receipt path under ``tmp_path``) are
    reproduced, matching ``tests/test_cli_relay_host.py``'s identical
    precedent.
    """
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_owned_session_pre_start_probe_observes_uninitialized_transition_without_writes(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"

    status = cli_session_owned._inspect_owned_session_recovery_before_start(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares",
        session_id="fresh-session",
        core_dir=tmp_path / "core",
        home=home,
        timeout_seconds=0.05,
    )

    assert status.cluster == "ares"
    assert status.session_id == "fresh-session"
    assert status.cleanup_receipt is False
    assert status.ownership_verified is False
    assert status.recovery_verified is False
    assert status.errors == [
        "owned session transition is not currently observable; "
        "start-owned remains the mutation authority"
    ]
    assert home.exists() is False


def test_owned_session_pre_start_probe_delegates_existing_transition_to_strict_recovery(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    transition_path = (
        home
        / ".local"
        / "share"
        / "clio-relay"
        / "sessions"
        / "existing-session"
        / "transition.lock"
    )
    transition_path.parent.mkdir(parents=True)
    transition_path.write_text("", encoding="utf-8")
    expected = OwnedSessionRecoveryStatus(
        cluster="ares",
        session_id="existing-session",
        cleanup_receipt=True,
        recovery_verified=True,
    )
    calls: list[dict[str, object]] = []

    def strict_recovery(**kwargs: object) -> OwnedSessionRecoveryStatus:
        calls.append(kwargs)
        return expected

    monkeypatch.setattr(
        cli_owned_session_recovery,
        "_inspect_owned_session_recovery_after_transition",
        strict_recovery,
    )

    observed = cli_session_owned._inspect_owned_session_recovery_before_start(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares",
        session_id="existing-session",
        core_dir=tmp_path / "core",
        home=home,
        timeout_seconds=12.5,
    )

    assert observed is expected
    assert calls == [
        {
            "cluster": "ares",
            "session_id": "existing-session",
            "core_dir": tmp_path / "core",
            "home": home,
            "timeout_seconds": 12.5,
        }
    ]


def test_cli_session_reopen_preserves_prior_generation_closure_history(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    prepared = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--candidate-generation-id",
            "generation-1",
        ],
    )
    quiesced = runner.invoke(
        app,
        [
            "session",
            "quiesce-intake",
            "--session-id",
            "session-1",
            "--session-generation-id",
            "generation-1",
        ],
    )
    closed = runner.invoke(
        app,
        [
            "session",
            "mark-closed",
            "--session-id",
            "session-1",
            "--session-generation-id",
            "generation-1",
        ],
    )
    reopened = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--recorded-generation-id",
            "generation-1",
            "--candidate-generation-id",
            "generation-2",
        ],
    )
    resumed = runner.invoke(
        app,
        [
            "session",
            "resume-intake",
            "--session-id",
            "session-1",
            "--session-generation-id",
            "generation-2",
        ],
    )

    queue = ClioCoreQueue(core_dir)
    assert prepared.exit_code == 0, prepared.output
    assert quiesced.exit_code == 0, quiesced.output
    assert closed.exit_code == 0, closed.output
    assert reopened.exit_code == 0, reopened.output
    assert resumed.exit_code == 0, resumed.output
    assert queue.owner_session_is_closing("session-1") is False
    assert (
        queue.get_owner_session_closed(
            "session-1",
            session_generation_id="generation-1",
        )
        is not None
    )
    assert (
        queue.get_owner_session_closed(
            "session-1",
            session_generation_id="generation-2",
        )
        is None
    )


def test_cli_session_prepare_start_preserves_active_generation_and_resources(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()
    first = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--candidate-generation-id",
            "generation-1",
        ],
    )
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="preserve-generation-job",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    gateway = queue.create_gateway_session(
        GatewaySession(
            cluster="ares",
            name="preserve-generation-gateway",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )

    replacement = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--recorded-generation-id",
            "generation-1",
            "--candidate-generation-id",
            "generation-2",
        ],
    )
    dead_api_recovery = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--candidate-generation-id",
            "generation-3",
        ],
    )

    assert first.exit_code == 0, first.output
    assert replacement.exit_code == 0, replacement.output
    assert dead_api_recovery.exit_code == 0, dead_api_recovery.output
    assert json.loads(replacement.output)["session_generation_id"] == "generation-1"
    assert json.loads(dead_api_recovery.output)["session_generation_id"] == "generation-1"
    assert queue.get_job(job.job_id).metadata["owner_session_generation_id"] == "generation-1"
    assert (
        queue.get_gateway_session(gateway.session_id).metadata["owner_session_generation_id"]
        == "generation-1"
    )


def test_cli_session_prepare_start_refuses_new_generation_before_closure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    _activate_owner_session(queue)
    queue.set_owner_session_closing(
        "session-1",
        session_generation_id="generation-1",
    )

    result = CliRunner().invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--recorded-generation-id",
            "generation-1",
            "--candidate-generation-id",
            "generation-2",
        ],
    )

    assert result.exit_code == 1
    assert "unfinished generation transition" in result.output
    assert queue.owner_session_is_closing("session-1") is True
    assert (
        queue.get_owner_session_closed(
            "session-1",
            session_generation_id="generation-1",
        )
        is None
    )

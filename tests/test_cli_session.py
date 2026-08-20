"""Tests for the user-facing ``session`` commands (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond import and
patch-target updates) alongside ``session_app``'s six user-facing commands'
extraction into ``src/clio_relay/cli_session.py``, per ground rule 3 (§2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through one of the moved commands moves with the logic it exercises.
``session start``/``session teardown`` stay in ``cli.py`` (see
``cli_session.py``'s own docstring for why), so every test exercising
either of those -- including ones that also happen to call ``session
detach``/``mark-closed``/etc. as setup or as a simulated remote-side call,
such as ``test_cli_acceptance_preflight_failure_always_writes_canonical_report``
and ``test_cli_cleanup_failure_report_preserves_requested_policy_from_command_
entry`` (both parametrized across several unrelated command groups) -- stays
in ``test_cli.py``.

``monkeypatch.setattr`` targets are unchanged from ``test_cli.py``:
``session_lifecycle.detach_remote_session``/``session_api.
submit_owned_session_job`` patch the owner module directly (already the
R8(i) idiom, unaffected by which file calls it); ``cli.
_cleanup_owned_runtime_sessions``/``cli._observe_worker_before_cleanup``
still patch ``cli.py`` because those helpers were never part of this
extraction -- they stay in ``cli.py``, shared with session teardown/detach
alike (see ``cli_session.py``'s own docstring).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.cli_owned_runtime_cleanup as cli_owned_runtime_cleanup
import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach
import clio_relay.session_api as session_api
import clio_relay.session_lifecycle as session_lifecycle
from clio_relay import cli
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.models import JarvisRunSpec, JobKind, RelayJob
from clio_relay.session_lifecycle import CleanupResource, SessionLifecycleReport
from tests.test_cli import (
    _activate_owner_session,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _fake_empty_runtime_cleanup,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _write_test_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
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


def test_cli_session_submit_jarvis_uses_identity_proven_client(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: acceptance\npkgs: []\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def submit_owned(**kwargs: object) -> RelayJob:
        captured.update(kwargs)
        selected_settings = cast(cli.RelaySettings, kwargs["settings"])
        payload = cast(dict[str, object], kwargs["payload"])
        return RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=cast(str, payload["pipeline_yaml"])),
            idempotency_key=cast(str, payload["idempotency_key"]),
            metadata={
                "owner": "clio-relay",
                "owner_session_id": selected_settings.owner_session_id,
                "owner_session_generation_id": selected_settings.owner_session_generation_id,
            },
        )

    monkeypatch.setattr(session_api, "submit_owned_session_job", submit_owned)

    result = CliRunner().invoke(
        app,
        [
            "session",
            "submit-jarvis",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--session-generation-id",
            "generation-1",
            "--pipeline-yaml-file",
            str(pipeline),
            "--idempotency-key",
            "acceptance-submit",
        ],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["metadata"]["owner_session_generation_id"] == "generation-1"
    settings = cast(cli.RelaySettings, captured["settings"])
    assert settings.api_token == "api-token"
    assert settings.owner_session_cluster == "ares"
    assert settings.remote_cluster is None
    assert settings.owner_session_id == "session-1"
    assert settings.owner_session_generation_id == "generation-1"
    assert cast(dict[str, object], captured["payload"])["pipeline_yaml"] == (
        "name: acceptance\npkgs: []\n"
    )


def test_cli_session_detach_never_records_owner_session_closure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    _activate_owner_session(ClioCoreQueue(core_dir))
    lifecycle_events: list[str] = []

    def fake_detach(**_kwargs: object) -> SessionLifecycleReport:
        lifecycle_events.append("observe_remote_api")
        return SessionLifecycleReport(
            cluster="ares",
            session_id="session-1",
            session_generation_id="generation-1",
            mode="detach",
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="ares",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                )
            ],
        )

    def fake_gateway_cleanup(**_kwargs: object) -> list[dict[str, object]]:
        lifecycle_events.append("cleanup_desktop_connectors")
        return []

    monkeypatch.setattr(session_lifecycle, "detach_remote_session", fake_detach)
    monkeypatch.setattr(
        cli_owned_runtime_cleanup, "_cleanup_owned_runtime_sessions", fake_gateway_cleanup
    )

    result = CliRunner().invoke(
        app,
        ["session", "detach", "--cluster", "ares", "--session-id", "session-1"],
    )

    queue = ClioCoreQueue(core_dir)
    assert result.exit_code == 0, result.output
    assert lifecycle_events == [
        "observe_remote_api",
        "cleanup_desktop_connectors",
        "observe_remote_api",
    ]
    assert queue.owner_session_is_closing("session-1") is False
    assert queue.get_owner_session_closed("session-1") is None


def test_cli_session_detach_reports_success_when_optional_worker_observation_times_out(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    report_path = tmp_path / "detach-worker-timeout.json"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_ARTIFACT_SHA256", "a" * 64)
    _activate_owner_session(ClioCoreQueue(core_dir))

    def retained_session(**_kwargs: object) -> SessionLifecycleReport:
        return SessionLifecycleReport(
            cluster="ares",
            session_id="session-1",
            session_generation_id="generation-1",
            mode="detach",
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="ares",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                )
            ],
        )

    def timed_out_worker_observation(
        _definition: ClusterDefinition,
    ) -> tuple[None, RelayError]:
        return None, RelayError("remote command timed out after 20 seconds: ares")

    monkeypatch.setattr(session_lifecycle, "detach_remote_session", retained_session)
    monkeypatch.setattr(
        cli_owned_runtime_cleanup, "_cleanup_owned_runtime_sessions", _fake_empty_runtime_cleanup
    )
    monkeypatch.setattr(
        cli_remote_worker_attach, "_observe_worker_before_cleanup", timed_out_worker_observation
    )

    result = CliRunner().invoke(
        app,
        [
            "session",
            "detach",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["validation_status"] == "failed"
    assert payload["validation_provenance_warning"] is True
    assert payload["residual_resources"] == []
    assert payload["resources"][0]["action"] == "retain"
    assert payload["resources"][0]["outcome"] == "retained"
    queue = ClioCoreQueue(core_dir)
    assert queue.owner_session_is_closing("session-1") is False
    assert queue.get_owner_session_closed("session-1") is None

    validation_report = json.loads(report_path.read_text(encoding="utf-8"))
    worker_check = next(
        check
        for check in validation_report["checks"]
        if check["check_id"] == "worker.installation-info"
    )
    assert validation_report["status"] == "failed"
    assert worker_check["status"] == "failed"
    assert "timed out after 20 seconds" in worker_check["error"]


def test_cli_session_detach_default_report_failure_controls_exit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))

    def incomplete_detach(**_kwargs: object) -> SessionLifecycleReport:
        return SessionLifecycleReport(
            cluster="ares",
            session_id="session-1",
            session_generation_id=None,
            mode="detach",
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="ares",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                )
            ],
        )

    def forbidden_gateway_cleanup(**_kwargs: object) -> list[dict[str, object]]:
        raise AssertionError("unverified detach must not mutate gateway connectors")

    monkeypatch.setattr(session_lifecycle, "detach_remote_session", incomplete_detach)
    monkeypatch.setattr(
        cli_owned_runtime_cleanup, "_cleanup_owned_runtime_sessions", forbidden_gateway_cleanup
    )

    result = CliRunner().invoke(
        app,
        ["session", "detach", "--cluster", "ares", "--session-id", "session-1"],
    )

    assert result.exit_code == 1
    reports = list((tmp_path / ".clio-relay" / "validation-reports").glob("*.json"))
    assert len(reports) == 1
    canonical = json.loads(reports[0].read_text(encoding="utf-8"))
    assert canonical["status"] == "failed"
    detach_check = next(
        check for check in canonical["checks"] if check["check_id"] == "cleanup.detach"
    )
    assert detach_check["status"] == "failed"


def test_cli_session_detach_rejects_generation_change_after_connector_cleanup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    observations = iter(("generation-1", "generation-2"))
    cleanup_calls: list[str] = []

    def changing_detach(**_kwargs: object) -> SessionLifecycleReport:
        generation_id = next(observations)
        return SessionLifecycleReport(
            cluster="ares",
            session_id="session-1",
            session_generation_id=generation_id,
            mode="detach",
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="ares",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                )
            ],
        )

    def record_gateway_cleanup(**_kwargs: object) -> list[dict[str, object]]:
        cleanup_calls.append("called")
        return []

    monkeypatch.setattr(session_lifecycle, "detach_remote_session", changing_detach)
    monkeypatch.setattr(
        cli_owned_runtime_cleanup, "_cleanup_owned_runtime_sessions", record_gateway_cleanup
    )

    result = CliRunner().invoke(
        app,
        ["session", "detach", "--cluster", "ares", "--session-id", "session-1"],
    )

    assert result.exit_code == 1
    assert "owned session generation changed during desktop detach" in result.output
    assert cleanup_calls == ["called"]
    reports = list((tmp_path / ".clio-relay" / "validation-reports").glob("*.json"))
    canonical = json.loads(reports[0].read_text(encoding="utf-8"))
    assert canonical["status"] == "failed"
    assert canonical["cleanup"]["remaining_resources"] == []


def test_cli_session_detach_failure_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def fail_detach(**_kwargs: object) -> SessionLifecycleReport:
        raise RelayError("remote session ownership check failed")

    monkeypatch.setattr(session_lifecycle, "detach_remote_session", fail_detach)
    report_path = tmp_path / "detach-failed.json"

    result = CliRunner().invoke(
        app,
        [
            "session",
            "detach",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["scenario"] == "cleanup"
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "session.detach"

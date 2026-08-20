"""Tests for the generic ``gateway`` CRUD commands (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond import updates)
alongside ``gateway_app``'s ``create``/``list``/``get``/``update``/``close``
extraction into ``src/clio_relay/cli_gateway.py``, per ground rule 3 (§2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through one of the moved commands moves with the logic it exercises. Tests
that exercise ``session teardown``/``session detach`` with a gateway session
merely as setup fixture data (e.g. ``test_gateway_scheduler_sentinel_
conflict_fails_before_any_destructive_cleanup``, ``test_owned_runtime_
cleanup_scans_remote_gateway_core``, and the cross-group-parametrized
``test_cli_cleanup_failure_report_preserves_requested_policy_from_command_
entry``) are not tests of this group's own logic and stay in ``test_cli.py``.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import GatewaySession, GatewaySessionState
from tests.test_cli import (
    _write_test_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only.

    That fixture also monkeypatches session-teardown-only collaborators; none
    of the tests in this file exercise that path, matching
    ``tests/test_cli_relay_host.py``'s/``tests/test_cli_session.py``'s
    identical precedent.
    """
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_cli_gateway_session_lifecycle(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    gateway_json = tmp_path / "gateway.json"
    gateway_json.write_text('{"strategy":"ssh_forward","remote_port":11111}', encoding="utf-8")
    resources_json = tmp_path / "resources.json"
    resources_json.write_text('{"nodes":1,"exclusive":true}', encoding="utf-8")

    created = CliRunner().invoke(
        app,
        [
            "gateway",
            "create",
            "--cluster",
            "test-cluster",
            "--name",
            "live-service-example",
            "--gateway-json-file",
            str(gateway_json),
            "--resources-json-file",
            str(resources_json),
            "--stdout-uri",
            "file:///tmp/stdout.log",
            "--stderr-uri",
            "file:///tmp/stderr.log",
            "--log-uri",
            "file:///tmp/service.log",
            "--artifact",
            "artifact://session/startup",
        ],
    )
    assert created.exit_code == 0
    session_id = json.loads(created.output)["session_id"]

    updated = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            session_id,
            "--state",
            "ready",
            "--node",
            "ares-comp-01",
            "--gateway-json",
            '{"strategy":"ssh_forward","local_port":5900}',
            "--resources-json",
            '{"nodes":2}',
            "--stdout-uri",
            "file:///tmp/updated-stdout.log",
            "--log-uri",
            "file:///tmp/updated.log",
            "--artifact",
            "artifact://session/updated",
        ],
    )
    listed = CliRunner().invoke(app, ["gateway", "list", "--cluster", "test-cluster"])
    closed = CliRunner().invoke(app, ["gateway", "close", session_id])

    assert updated.exit_code == 0
    assert listed.exit_code == 0
    assert closed.exit_code == 0
    assert json.loads(updated.output)["state"] == GatewaySessionState.READY.value
    assert json.loads(created.output)["gateway"]["remote_port"] == 11111
    assert json.loads(created.output)["requested_resources"]["exclusive"] is True
    assert json.loads(created.output)["stdout_uri"] == "file:///tmp/stdout.log"
    assert json.loads(created.output)["log_uris"] == ["file:///tmp/service.log"]
    assert json.loads(created.output)["artifacts"] == ["artifact://session/startup"]
    assert json.loads(updated.output)["requested_resources"] == {"nodes": 2}
    assert json.loads(updated.output)["scheduler_job_id"] is None
    assert json.loads(updated.output)["stdout_uri"] == "file:///tmp/updated-stdout.log"
    assert json.loads(updated.output)["log_uris"] == ["file:///tmp/updated.log"]
    assert json.loads(updated.output)["artifacts"] == ["artifact://session/updated"]
    listed_page = json.loads(listed.output)
    assert listed_page["gateway_sessions"][0]["session_id"] == session_id
    assert listed_page["source_cursor"] == 1
    assert listed_page["source_limit"] == 100
    assert listed_page["source_next_cursor"] is None
    assert listed_page["source_total"] == 1
    assert json.loads(closed.output)["state"] == GatewaySessionState.CLOSED.value


def test_cli_remote_gateway_passthrough_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    gateway_json = tmp_path / "gateway.json"
    gateway_json.write_text('{"strategy":"ssh_forward","remote_port":11111}', encoding="utf-8")
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
        return subprocess.CompletedProcess(
            command,
            0,
            (b'{"session_id":"gateway_remote","cluster":"ares","name":"live-service-example"}\n'),
            b"",
        )

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    created = CliRunner().invoke(
        app,
        [
            "gateway",
            "create",
            "--cluster",
            "ares",
            "--name",
            "live-service-example",
            "--gateway-json-file",
            str(gateway_json),
        ],
    )
    updated = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            "gateway_remote",
            "--cluster",
            "ares",
            "--state",
            "ready",
            "--node",
            "ares-comp-01",
        ],
    )
    closed = CliRunner().invoke(app, ["gateway", "close", "gateway_remote", "--cluster", "ares"])

    assert created.exit_code == 0
    assert updated.exit_code == 0
    assert closed.exit_code == 0
    assert [json.loads(item.output)["session_id"] for item in [created, updated, closed]] == [
        "gateway_remote",
        "gateway_remote",
        "gateway_remote",
    ]
    assert '"$HOME/.local/bin/clio-relay" gateway create' in commands[0][2]
    assert "remote_port" in commands[0][2]
    assert '"$HOME/.local/bin/clio-relay" gateway update gateway_remote' in commands[1][2]
    assert '"$HOME/.local/bin/clio-relay" gateway close gateway_remote' in commands[2][2]


@pytest.mark.parametrize(
    "command",
    [
        [
            "gateway",
            "create",
            "--cluster",
            "ares",
            "--name",
            "forged-runtime",
            "--scheduler",
            "slurm",
        ],
        [
            "gateway",
            "update",
            "gateway_target",
            "--scheduler-job-id",
            "12345",
        ],
    ],
)
def test_cli_generic_gateway_commands_have_no_scheduler_identity_arguments(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    command: list[str],
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))

    result = CliRunner().invoke(app, command)

    assert result.exit_code != 0
    assert "No such option" in result.output
    assert ClioCoreQueue(tmp_path / "core").list_gateway_sessions() == []


@pytest.mark.parametrize(
    ("option", "payload", "protected_field"),
    [
        ("--gateway-json", '{"runtime_spec":{"kind":"forged"}}', "gateway.runtime_spec"),
        (
            "--gateway-json",
            '{"jarvis_runtime_binding":{"schema_version":"forged"}}',
            "gateway.jarvis_runtime_binding",
        ),
        (
            "--gateway-json",
            '{"transport":{"remote_connector":{"pid":42}}}',
            "gateway.transport.remote_connector",
        ),
        ("--metadata-json", '{"owner":"clio-relay"}', "metadata.owner"),
    ],
)
def test_cli_generic_gateway_create_rejects_runtime_owned_json(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    option: str,
    payload: str,
    protected_field: str,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))

    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "create",
            "--cluster",
            "ares",
            "--name",
            "forged-runtime",
            option,
            payload,
        ],
    )

    assert result.exit_code != 0
    assert protected_field in result.output
    assert ClioCoreQueue(tmp_path / "core").list_gateway_sessions() == []


def test_cli_generic_gateway_update_cannot_replace_relay_runtime_state(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    runtime = queue.create_gateway_session(
        GatewaySession(
            cluster="ares",
            name="relay-runtime",
            gateway={
                "runtime_spec": {"kind": "image-service"},
                "ownership_intents": {"scheduler_submission": {"state": "recorded"}},
            },
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )

    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            runtime.session_id,
            "--gateway-json",
            '{"strategy":"ssh_forward"}',
        ],
    )

    assert result.exit_code == 1
    assert "cannot replace relay-managed runtime state" in result.stderr
    assert queue.get_gateway_session(runtime.session_id).gateway == runtime.gateway


def test_cli_generic_gateway_update_preserves_ordinary_gateway_mutations(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    created = CliRunner().invoke(
        app,
        ["gateway", "create", "--cluster", "ares", "--name", "ordinary-gateway"],
    )
    session_id = json.loads(created.output)["session_id"]

    updated = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            session_id,
            "--gateway-json",
            '{"strategy":"ssh_forward","local_port":5900}',
            "--metadata-json",
            '{"dataset":"example"}',
        ],
    )

    assert created.exit_code == 0
    assert updated.exit_code == 0
    payload = json.loads(updated.output)
    assert payload["gateway"] == {"strategy": "ssh_forward", "local_port": 5900}
    assert payload["metadata"] == {"dataset": "example"}


def test_cli_gateway_update_closed_session_reports_clean_error(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    created = CliRunner().invoke(
        app,
        ["gateway", "create", "--cluster", "ares", "--name", "live-service-example"],
    )
    assert created.exit_code == 0
    session_id = json.loads(created.output)["session_id"]

    closed = CliRunner().invoke(app, ["gateway", "close", session_id])
    updated = CliRunner().invoke(app, ["gateway", "update", session_id, "--state", "ready"])

    assert closed.exit_code == 0
    assert updated.exit_code == 1
    assert "error: cannot reopen closed gateway session" in updated.stderr
    assert "Traceback" not in updated.output
    assert "Traceback" not in updated.stderr

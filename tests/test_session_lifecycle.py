from __future__ import annotations

import json
import subprocess

from pytest import MonkeyPatch

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.session_lifecycle import (
    start_remote_session,
    status_remote_session,
    teardown_remote_session,
)


def test_start_remote_session_writes_owned_pid_and_metadata(monkeypatch: MonkeyPatch) -> None:
    calls: list[tuple[list[str], str]] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        calls.append((command, input.decode("utf-8")))
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(
            command,
            0,
            b"session_started=session-1\napi_pid=123\nremote_api_port=9001\n",
            b"",
        )

    monkeypatch.setattr("clio_relay.session_lifecycle.subprocess.run", fake_run)

    lines = start_remote_session(
        cluster="ares",
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        remote_api_port=9001,
        api_token="token",
    )

    assert "session_started=session-1" in lines
    assert calls[0][0] == ["ssh", "ares", "bash", "-s"]
    script = calls[0][1]
    assert "CLIO_RELAY_API_TOKEN='token'" in script
    assert "clio-relay api start --host 127.0.0.1 --port 9001 --require-token" in script
    assert "api.pid" in script
    assert "metadata.json" in script
    assert "pkill" not in script


def test_status_remote_session_returns_json(monkeypatch: MonkeyPatch) -> None:
    def fake_run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        del input, capture_output, check
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps({"session_id": "session-1", "running": True}).encode("utf-8"),
            b"",
        )

    monkeypatch.setattr("clio_relay.session_lifecycle.subprocess.run", fake_run)

    status = status_remote_session(
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
    )

    assert status == {"session_id": "session-1", "running": True}


def test_teardown_remote_session_kills_owned_pid_and_optional_worker(
    monkeypatch: MonkeyPatch,
) -> None:
    scripts: list[str] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        del capture_output, check
        scripts.append(input.decode("utf-8"))
        return subprocess.CompletedProcess(
            command,
            0,
            b"api_stopped=123\nworker_stopped=clio-relay-worker-ares.service\n",
            b"",
        )

    monkeypatch.setattr("clio_relay.session_lifecycle.subprocess.run", fake_run)

    lines = teardown_remote_session(
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        stop_worker=True,
        cluster="ares",
    )

    assert "api_stopped=123" in lines
    assert "worker_stopped=clio-relay-worker-ares.service" in lines
    assert 'kill "$api_pid"' in scripts[0]
    assert "systemctl --user stop clio-relay-worker-ares.service" in scripts[0]

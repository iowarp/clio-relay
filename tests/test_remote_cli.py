from __future__ import annotations

import subprocess

from pytest import MonkeyPatch, raises

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.remote_cli import (
    remote_command_timeout,
    remote_env,
    remove_remote_file,
    run_remote_shell,
    write_remote_file,
)


def test_remote_env_exports_operator_configured_jarvis_spack_executable() -> None:
    rendered = remote_env(
        ClusterDefinition(
            name="ares",
            ssh_host="ares-login",
            spack_executable="/home/operator/spack/bin/spack",
        )
    )

    assert "export JARVIS_MCP_SPACK_COMMAND=/home/operator/spack/bin/spack;" in rendered
    assert 'export UV="$HOME/.local/bin/uv";' in rendered
    assert 'export CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE="$HOME/.local/bin/clio-relay";' in rendered


def test_remote_env_forwards_nonsecret_validation_provenance(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN", "release-operator")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID", "123456")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_INVOCATION_ID", "candidate report 17")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_LAUNCHER", "uv-tool")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_ARTIFACT_SHA256", "a" * 64)
    monkeypatch.setenv(
        "CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE",
        r"C:\local\tool-bin\clio-relay.exe",
    )
    monkeypatch.setenv("UV", r"C:\local\uv.exe")

    rendered = remote_env(ClusterDefinition(name="cluster-a", ssh_host="cluster-a-login"))

    assert "export CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN=release-operator;" in rendered
    assert "export CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID=123456;" in rendered
    assert "export CLIO_RELAY_VALIDATION_INVOCATION_ID='candidate report 17';" in rendered
    assert "export CLIO_RELAY_VALIDATION_LAUNCHER=uv-tool;" in rendered
    assert f"export CLIO_RELAY_VALIDATION_ARTIFACT_SHA256={'a' * 64};" in rendered
    assert r"C:\local\tool-bin\clio-relay.exe" not in rendered
    assert r"C:\local\uv.exe" not in rendered


def test_remote_staging_uses_private_modes_and_literal_quoted_paths(
    monkeypatch: MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    shell_scripts: list[str] = []
    ssh_commands: list[list[str]] = []

    def run_shell(_definition: ClusterDefinition, script: str) -> str:
        shell_scripts.append(script)
        return ""

    monkeypatch.setattr(
        "clio_relay.remote_cli.run_remote_shell",
        run_shell,
    )

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        del kwargs
        ssh_commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout=b"", stderr=b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", run)
    path = ".local/share/clio-relay/private run/arguments;not-a-command.json"

    write_remote_file(definition, path, b'{"token":"private"}')
    remove_remote_file(definition, path, remove_empty_parent=True)

    assert shell_scripts[0].startswith("umask 077; mkdir -p ")
    assert "chmod 700 '.local/share/clio-relay/private run'" in shell_scripts[0]
    assert ssh_commands == [
        [
            "ssh",
            "ares-login",
            "umask 077; cat > '.local/share/clio-relay/private run/arguments;not-a-command.json' "
            "&& chmod 600 '.local/share/clio-relay/private run/arguments;not-a-command.json'",
        ]
    ]
    assert shell_scripts[1] == (
        "rm -f -- '.local/share/clio-relay/private run/arguments;not-a-command.json' && { "
        "rmdir -- '.local/share/clio-relay/private run' 2>/dev/null || true; }"
    )


def test_remote_staging_deletion_failure_propagates(monkeypatch: MonkeyPatch) -> None:
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")

    def fail_remove(_definition: ClusterDefinition, script: str) -> str:
        assert script.startswith("rm -f -- ")
        assert " && { rmdir -- " in script
        raise RelayError("remote file removal failed")

    monkeypatch.setattr("clio_relay.remote_cli.run_remote_shell", fail_remove)

    with raises(RelayError, match="remote file removal failed"):
        remove_remote_file(
            definition,
            ".local/share/clio-relay/private/arguments.json",
            remove_empty_parent=True,
        )


def test_bounded_remote_command_timeout_is_translated(monkeypatch: MonkeyPatch) -> None:
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")

    def timed_out(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        assert capture_output is True
        assert check is False
        assert timeout == 12
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", timed_out)

    with raises(RelayError, match="timed out after 12 seconds"), remote_command_timeout(12):
        run_remote_shell(definition, "true")

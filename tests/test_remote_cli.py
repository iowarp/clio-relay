from __future__ import annotations

import subprocess

from pytest import MonkeyPatch, raises

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.remote_cli import remove_remote_file, write_remote_file


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

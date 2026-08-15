from __future__ import annotations

import subprocess

from pytest import MonkeyPatch, raises

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.remote_cli import (
    remote_command_timeout,
    remote_env,
    remove_remote_file,
    run_remote_clio,
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

    assert 'export JARVIS_MCP_SPACK_COMMAND="/home/operator/spack/bin/spack";' in rendered
    assert 'export UV="$HOME/.local/bin/uv";' in rendered
    assert 'export CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE="$HOME/.local/bin/clio-relay";' in rendered


def test_remote_clio_uses_the_configured_digest_bound_executable(
    monkeypatch: MonkeyPatch,
) -> None:
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares-login",
        relay_executable="/srv/releases/relay-a1b2/bin/clio-relay",
        relay_install_receipt="/srv/releases/relay-a1b2/install-receipt.json",
    )
    observed: list[str] = []

    def run_shell(_definition: ClusterDefinition, script: str) -> str:
        observed.append(script)
        return "{}"

    monkeypatch.setattr("clio_relay.remote_cli.run_remote_shell", run_shell)

    assert run_remote_clio(definition, ["queue", "list"]) == "{}"
    assert observed == [
        remote_env(definition) + ' "/srv/releases/relay-a1b2/bin/clio-relay" queue list'
    ]
    assert (
        'export CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE="/srv/releases/relay-a1b2/bin/clio-relay";'
    ) in observed[0]
    assert (
        'export CLIO_RELAY_INSTALL_RECEIPT="/srv/releases/relay-a1b2/install-receipt.json";'
    ) in observed[0]


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

    with (
        raises(ObservationTimeoutError, match="timed out after 12 seconds"),
        remote_command_timeout(12),
    ):
        run_remote_shell(definition, "true")


def test_run_remote_shell_surfaces_a_delivery_refusal_by_its_own_message(
    monkeypatch: MonkeyPatch,
) -> None:
    """A1 (#231 R6-fix review): a remote CLI guard (e.g. ``job read-artifact``)

    exits non-zero *after* printing a T2 delivery-refusal document (doc
    §6.4) to stdout -- ``run_remote_shell`` must recognize it and surface
    its own typed code/message, not the generic "remote command failed:
    <raw blob>" a blanket non-zero-exit check would otherwise report.
    Exercised through the real ``subprocess.run`` seam (the exact pattern
    ``test_bounded_remote_command_timeout_is_translated`` uses above), not
    by calling a helper in isolation.
    """
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    refusal = (
        b'{"content_truncated": true, "result_available": false, "delivery": '
        b'{"schema_version": "clio-relay.mcp-result-delivery.v1", "status": "failed", '
        b'"code": "artifact_content_too_large", "max_inline_bytes": 16777216, '
        b'"private_evidence_preserved": true, '
        b'"remote_side_effects_may_have_occurred": false, '
        b'"message": "artifact content exceeds the 16777216-byte transfer limit"}}'
    )

    def fake_run(
        command: list[str], *, capture_output: bool, check: bool
    ) -> subprocess.CompletedProcess[bytes]:
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 1, stdout=refusal, stderr=b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    with raises(
        RelayError,
        match=r"remote command refused delivery \(artifact_content_too_large\): "
        r"artifact content exceeds the 16777216-byte transfer limit",
    ):
        run_remote_shell(definition, "clio-relay job read-artifact a1")


def test_run_remote_shell_falls_back_to_the_generic_error_for_a_non_refusal_failure(
    monkeypatch: MonkeyPatch,
) -> None:
    """A real command failure (not a delivery refusal) still reports the raw blob."""
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")

    def fake_run(
        command: list[str], *, capture_output: bool, check: bool
    ) -> subprocess.CompletedProcess[bytes]:
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 127, stdout=b"", stderr=b"command not found")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    with raises(RelayError, match="remote command failed: command not found"):
        run_remote_shell(definition, "not-a-real-command")

"""The bootstrap preflight must not send its script as a command-line argument.

Found driving the ares self-install (clio-relay#158). The preflight embedded
its ~13 KB script in argv:

    _run(["ssh", ssh_host, remote_command])

Some ssh clients silently TRUNCATE a long command-line argument. The MSYS2
OpenSSH shipped with Git for Windows -- the default `ssh` on a Windows
developer box -- drops everything past roughly 8-10 KB. The remote shell then
receives a script cut off mid-token and reports

    bash: -c: line 11: unexpected EOF while looking for matching `''

which names neither the truncation nor the transport, so a client-side
transport limit is misread as a malformed script. Measured on the real host:
an 8117-byte command arrived intact, a 10117-byte command did not.

Size of the payload must not be a function of argv, so the script travels over
stdin -- the same shape ``session_remote_scripts._ssh_script`` already uses.
"""

from __future__ import annotations

from typing import Any, cast

from pytest import MonkeyPatch

import clio_relay.bootstrap as bootstrap


class _Captured:
    def __init__(self) -> None:
        self.command: list[str] = []
        self.input_bytes: bytes | None = None


def _capture(monkeypatch: MonkeyPatch) -> _Captured:
    captured = _Captured()

    def fake_run(
        command: list[str],
        *,
        input_bytes: bytes | None = None,
        **_kwargs: object,
    ) -> Any:
        captured.command = command
        captured.input_bytes = input_bytes

        class _Result:
            stdout = "bootstrap_preflight_unsupported=not_installed\n"
            stderr = ""
            returncode = 0

        return _Result()

    monkeypatch.setattr(bootstrap, "_run", fake_run)
    return captured


def _desired_state() -> Any:
    from clio_relay.bootstrap_reconcile import BootstrapDesiredState

    return cast(
        Any,
        BootstrapDesiredState.model_validate(
            {
                "schema_version": "clio-relay.bootstrap-desired-state.v1",
                "bootstrap_profile": "linux-user",
                "cluster": "ares-p5run2",
                "core_dir": "/mnt/common/u/relay-core",
                "spool_dir": "/mnt/common/u/relay-spool",
                "relay_install_spec": "clio-relay==1.6.6",
                "relay_artifact_sha256": "a" * 64,
                "relay_source_identity": "release:clio-relay==1.6.6:sha256:" + "a" * 64,
                "uv_version": "0.11.28",
                "uv_sha256": "b" * 64,
                "frp_version": "0.69.1",
                "frpc_sha256": "c" * 64,
                "frps_sha256": "d" * 64,
                "jarvis_cd_version": "1.8.0",
                "jarvis_cd_wheel_url": "https://example.test/jarvis_cd-1.8.0-py3-none-any.whl",
                "jarvis_cd_wheel_sha256": "e" * 64,
                "jarvis_util_commit": "f" * 40,
                "clio_kit_version": "2.7.2",
                "clio_kit_install_spec": "https://example.test/clio_kit-2.7.2-py3-none-any.whl",
                "clio_kit_artifact_sha256": "0" * 64,
                "jarvis_root": "~/.ppi-jarvis",
                "jarvis_config_dir": "~/.local/share/clio-relay/jarvis-config",
                "jarvis_private_dir": "~/.local/share/clio-relay/jarvis-private",
                "jarvis_shared_dir": "~/.local/share/clio-relay/jarvis-shared",
                "jarvis_resource_graph_profile": "ares",
                "allow_jarvis_resource_graph_build": False,
                "managed_jarvis_repo": "~/.local/share/clio-relay/clio_relay",
                "agent_adapter": "exec",
                "agent_args": [],
                "agent_npm_bin": None,
                "agent_npm_package": None,
                "worker_service": "clio-relay-worker-ares-p5run2.service",
            }
        ),
    )


def test_preflight_sends_its_script_over_stdin_not_argv(monkeypatch: MonkeyPatch) -> None:
    captured = _capture(monkeypatch)

    bootstrap._bootstrap_preflight_over_ssh(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ssh_host="ares",
        invocation_id="bootstrap_test",
        desired=_desired_state(),
        core_dir="/mnt/common/u/relay-core",
        spool_dir="/mnt/common/u/relay-spool",
        repair=False,
        timeout_seconds=30.0,
    )

    assert captured.command == ["ssh", "ares", "bash", "-s"]
    assert captured.input_bytes is not None
    # The script is real, and large enough that argv delivery was the hazard.
    assert b"bootstrap_preflight_unsupported=not_installed" in captured.input_bytes
    assert len(captured.input_bytes) > 8 * 1024


def test_no_preflight_argument_can_be_truncated_by_a_client_limit(
    monkeypatch: MonkeyPatch,
) -> None:
    """No single argv element may carry script-sized content.

    Guards the property rather than the current shape: whatever the command
    becomes, its arguments stay far below the ~8 KB where real ssh clients
    start dropping bytes.
    """
    captured = _capture(monkeypatch)

    bootstrap._bootstrap_preflight_over_ssh(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ssh_host="ares",
        invocation_id="bootstrap_test",
        desired=_desired_state(),
        core_dir="/mnt/common/u/relay-core",
        spool_dir="/mnt/common/u/relay-spool",
        repair=False,
        timeout_seconds=30.0,
    )

    assert max(len(argument) for argument in captured.command) < 1024

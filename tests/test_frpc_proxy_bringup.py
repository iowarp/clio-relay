"""Tests for the frpc proxy ssh-execution layer (clio-relay#279).

Follows ``test_bootstrap_preflight_transport.py``'s harness pattern exactly:
``subprocess.run`` is monkeypatched on the OWNER module
(``frpc_proxy_bringup.subprocess.run``), never a real dial. Every test here
asserts the exact argv (``["ssh", host, "bash", "-s"]``, one dial, script
over stdin -- never argv) and the parsed typed result.
"""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from clio_relay import frpc_proxy_bringup
from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.errors import RelayError
from clio_relay.frpc_proxy_receipt import BRINGUP_RECEIPT_SCHEMA, TEARDOWN_RECEIPT_SCHEMA
from clio_relay.frpc_unit import frpc_proxy_paths


def _definition(*, cluster: str = "ares") -> ClusterDefinition:
    return ClusterDefinition(
        name=cluster,
        ssh_host=f"{cluster}-login",
        frp_transport=FrpTransportConfig(
            server_addr="relay.example.org",
            identity_anchor="preshared_link_secret",
        ),
    )


@pytest.fixture(autouse=True)
def _frp_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "TOKEN-VALUE-ABC")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "SECRET-VALUE-XYZ")


class _Captured:
    def __init__(self) -> None:
        self.command: list[str] = []
        self.input_bytes: bytes | None = None
        self.timeout: float | None = None


def _fake_run(captured: _Captured, *, stdout: str, returncode: int = 0) -> Any:
    def fake(
        command: list[str],
        *,
        input: bytes | None = None,  # noqa: A002
        capture_output: bool = True,
        check: bool = False,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        del capture_output, check
        captured.command = command
        captured.input_bytes = input
        captured.timeout = timeout
        return subprocess.CompletedProcess(command, returncode, stdout.encode("utf-8"), b"")

    return fake


def _bringup_stdout(
    paths: Any,
    *,
    cluster: str = "ares",
    proxy_name: str = "ares-owned-session",
    active: str = "true",
    linger: str = "yes",
    persistence: str = "systemd-user-linger",
) -> str:
    return "\n".join(
        [
            f"FrpcProxyReceiptSchema={BRINGUP_RECEIPT_SCHEMA}",
            f"FrpcProxyCluster={cluster}",
            f"FrpcProxyName={proxy_name}",
            f"FrpcProxyUnitName={paths.unit_name}",
            f"FrpcProxyTomlPath={paths.toml_unit_path}",
            f"FrpcProxyEnvPath={paths.env_unit_path}",
            f"FrpcProxyConfigSha256={'a' * 64}",
            "FrpcProxyEnabled=true",
            f"FrpcProxyActive={active}",
            f"FrpcProxyLinger={linger}",
            f"FrpcProxyPersistence={persistence}",
            "FrpcProxyInstalledAt=2026-08-26T00:00:00Z",
            "",
        ]
    )


def _teardown_stdout(paths: Any, *, cluster: str = "ares") -> str:
    return "\n".join(
        [
            f"FrpcProxyTeardownSchema={TEARDOWN_RECEIPT_SCHEMA}",
            f"FrpcProxyCluster={cluster}",
            f"FrpcProxyUnitName={paths.unit_name}",
            "FrpcProxyRemovedUnit=true",
            "FrpcProxyRemovedToml=true",
            "FrpcProxyRemovedEnv=true",
            "FrpcProxyTornDownAt=2026-08-26T00:05:00Z",
            "",
        ]
    )


# --- install ------------------------------------------------------------


def test_install_dials_exactly_once_over_stdin_and_returns_the_typed_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = _definition()
    paths = frpc_proxy_paths("ares")
    captured = _Captured()
    monkeypatch.setattr(
        frpc_proxy_bringup.subprocess, "run", _fake_run(captured, stdout=_bringup_stdout(paths))
    )

    receipt = frpc_proxy_bringup.install_frpc_proxy_over_ssh(
        cluster="ares", definition=definition, ssh_host="ares-login"
    )

    assert captured.command == ["ssh", "ares-login", "bash", "-s"]
    assert captured.input_bytes is not None
    assert b"clio_relay_endpoint_activate_bounded" in captured.input_bytes
    assert receipt.cluster == "ares"
    assert receipt.unit_name == paths.unit_name
    assert receipt.enabled is True
    assert receipt.active is True
    assert receipt.linger is True
    assert receipt.persistence == "systemd-user-linger"


def test_install_never_puts_the_script_in_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    """Guards the property, not just the current shape (mirrors #158's own test)."""
    definition = _definition()
    paths = frpc_proxy_paths("ares")
    captured = _Captured()
    monkeypatch.setattr(
        frpc_proxy_bringup.subprocess, "run", _fake_run(captured, stdout=_bringup_stdout(paths))
    )

    frpc_proxy_bringup.install_frpc_proxy_over_ssh(
        cluster="ares", definition=definition, ssh_host="ares-login"
    )

    assert max(len(argument) for argument in captured.command) < 1024


def test_install_raises_on_a_nonzero_remote_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = _definition()
    captured = _Captured()

    def fake(
        command: list[str],
        *,
        input: bytes | None = None,
        **_kwargs: object,  # noqa: A002
    ) -> subprocess.CompletedProcess[bytes]:
        captured.command = command
        return subprocess.CompletedProcess(
            command, 1, b"", b"frpc proxy unit did not become active\n"
        )

    monkeypatch.setattr(frpc_proxy_bringup.subprocess, "run", fake)

    with pytest.raises(RelayError, match="failed to install frpc proxy"):
        frpc_proxy_bringup.install_frpc_proxy_over_ssh(
            cluster="ares", definition=definition, ssh_host="ares-login"
        )


def test_install_raises_a_typed_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = _definition()

    def fake(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(cmd=command, timeout=1.0)

    monkeypatch.setattr(frpc_proxy_bringup.subprocess, "run", fake)

    with pytest.raises(RelayError, match="exceeded"):
        frpc_proxy_bringup.install_frpc_proxy_over_ssh(
            cluster="ares", definition=definition, ssh_host="ares-login", timeout_seconds=1.0
        )


def test_install_rejects_an_ssh_host_that_looks_like_an_option(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = _definition()

    def poison(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("must not dial with an unsafe ssh destination")

    monkeypatch.setattr(frpc_proxy_bringup.subprocess, "run", poison)

    with pytest.raises(RelayError, match="ssh host"):
        frpc_proxy_bringup.install_frpc_proxy_over_ssh(
            cluster="ares", definition=definition, ssh_host="-oProxyCommand=evil"
        )


def test_install_propagates_a_missing_identity_anchor_before_any_dial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares-login",
        frp_transport=FrpTransportConfig(server_addr="relay.example.org"),
    )

    def poison(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("must refuse before dialing when the identity anchor is missing")

    monkeypatch.setattr(frpc_proxy_bringup.subprocess, "run", poison)

    with pytest.raises(Exception, match="identity anchor"):
        frpc_proxy_bringup.install_frpc_proxy_over_ssh(
            cluster="ares", definition=definition, ssh_host="ares-login"
        )


# --- D6 (Python layer): never trust rc==0 + a parsed receipt alone --------


def test_install_raises_when_the_receipt_reports_an_inactive_unit_despite_rc_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """D6 blocker: exit-0-with-active=false must never be reported as success.

    The script's own D6 guard (``frpc_proxy_scripts.py``) already refuses to
    print a receipt for an inactive unit, but this is a SECOND, independent
    check: if a receipt somehow reaches here with ``active=false`` anyway
    (an older script, a future regression), this must raise -- never return
    the receipt as if it were a success -- and the message must carry the
    receipt's own content so the journal pointer survives.
    """
    definition = _definition()
    paths = frpc_proxy_paths("ares")
    captured = _Captured()
    monkeypatch.setattr(
        frpc_proxy_bringup.subprocess,
        "run",
        _fake_run(captured, stdout=_bringup_stdout(paths, active="false")),
    )

    with pytest.raises(RelayError, match="not active") as excinfo:
        frpc_proxy_bringup.install_frpc_proxy_over_ssh(
            cluster="ares", definition=definition, ssh_host="ares-login"
        )

    assert "proxy-status" in str(excinfo.value)
    assert paths.unit_name in str(excinfo.value)
    # The receipt's own content survives in the message (journal pointer).
    assert '"active":false' in str(excinfo.value)


def test_install_succeeds_when_the_receipt_reports_a_genuinely_active_unit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = _definition()
    paths = frpc_proxy_paths("ares")
    captured = _Captured()
    monkeypatch.setattr(
        frpc_proxy_bringup.subprocess,
        "run",
        _fake_run(captured, stdout=_bringup_stdout(paths, active="true")),
    )

    receipt = frpc_proxy_bringup.install_frpc_proxy_over_ssh(
        cluster="ares", definition=definition, ssh_host="ares-login"
    )

    assert receipt.active is True


# --- D3: timeout must exceed the reused activation observer's own bound ----


def test_install_default_timeout_exceeds_the_activation_observers_own_bound() -> None:
    """D3 blocker: a local ssh bound shorter than the observer's own bound kills

    ssh mid-script in exactly the slow case the observer exists to ride out,
    leaving unbounded partial remote state and no receipt at all.
    """
    from clio_relay.deployment_activation import ENDPOINT_SERVICE_START_OBSERVATION_TIMEOUT_SECONDS

    assert (
        frpc_proxy_bringup.FRPC_PROXY_INSTALL_SSH_TIMEOUT_SECONDS
        > ENDPOINT_SERVICE_START_OBSERVATION_TIMEOUT_SECONDS
    )
    assert frpc_proxy_bringup.FRPC_PROXY_INSTALL_SSH_TIMEOUT_SECONDS == 420.0


def test_install_uses_the_derived_timeout_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = _definition()
    paths = frpc_proxy_paths("ares")
    captured = _Captured()
    monkeypatch.setattr(
        frpc_proxy_bringup.subprocess, "run", _fake_run(captured, stdout=_bringup_stdout(paths))
    )

    frpc_proxy_bringup.install_frpc_proxy_over_ssh(
        cluster="ares", definition=definition, ssh_host="ares-login"
    )

    assert captured.timeout == frpc_proxy_bringup.FRPC_PROXY_INSTALL_SSH_TIMEOUT_SECONDS


# --- teardown -------------------------------------------------------------


def test_teardown_dials_exactly_once_and_returns_the_typed_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = frpc_proxy_paths("ares")
    captured = _Captured()
    monkeypatch.setattr(
        frpc_proxy_bringup.subprocess, "run", _fake_run(captured, stdout=_teardown_stdout(paths))
    )

    receipt = frpc_proxy_bringup.teardown_frpc_proxy_over_ssh(cluster="ares", ssh_host="ares-login")

    assert captured.command == ["ssh", "ares-login", "bash", "-s"]
    assert receipt.unit_name == paths.unit_name
    assert receipt.removed_unit is True


def test_teardown_raises_on_a_nonzero_remote_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(command, 2, b"", b"disable failed\n")

    monkeypatch.setattr(frpc_proxy_bringup.subprocess, "run", fake)

    with pytest.raises(RelayError, match="failed to teardown frpc proxy"):
        frpc_proxy_bringup.teardown_frpc_proxy_over_ssh(cluster="ares", ssh_host="ares-login")


# --- status -----------------------------------------------------------------


def test_status_dials_exactly_once_and_returns_a_classified_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = frpc_proxy_paths("ares")
    stdout = "\n".join(
        [
            "LoadState=loaded",
            "ActiveState=active",
            "SubState=running",
            "UnitFileState=enabled",
            "JournalTailBase64=",
            "",
        ]
    )
    captured = _Captured()
    monkeypatch.setattr(frpc_proxy_bringup.subprocess, "run", _fake_run(captured, stdout=stdout))

    document = frpc_proxy_bringup.frpc_proxy_status_over_ssh(cluster="ares", ssh_host="ares-login")

    assert captured.command == ["ssh", "ares-login", "bash", "-s"]
    assert document.cluster == "ares"
    assert document.unit_name == paths.unit_name
    assert document.active is True
    assert document.diagnosis == "frpc proxy unit is active"


def test_status_surfaces_a_typed_reason_for_a_never_installed_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout = "\n".join(
        [
            "LoadState=not-found",
            "ActiveState=inactive",
            "SubState=dead",
            "UnitFileState=disabled",
            "JournalTailBase64=",
            "",
        ]
    )
    captured = _Captured()
    monkeypatch.setattr(frpc_proxy_bringup.subprocess, "run", _fake_run(captured, stdout=stdout))

    document = frpc_proxy_bringup.frpc_proxy_status_over_ssh(cluster="ares", ssh_host="ares-login")

    assert document.installed is False
    assert "install-proxy" in document.diagnosis

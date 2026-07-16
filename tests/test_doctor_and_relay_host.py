from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import cast

import pytest

import clio_relay.deployment as deployment
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.deployment import (
    install_endpoint_user_service_over_ssh,
    render_endpoint_user_service,
)
from clio_relay.doctor import run_doctor
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpsConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
    render_frps_config,
)


def test_render_frps_config_has_no_application_state() -> None:
    rendered = render_frps_config(
        FrpsConfig(
            bind_port=7001,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
        )
    )

    assert "bindPort = 7001" in rendered
    assert 'auth.token = "secret"' in rendered
    assert "job" not in rendered.lower()
    assert "queue" not in rendered.lower()


def test_render_frpc_config_uses_configured_websocket_transport() -> None:
    rendered = render_frpc_config(
        FrpcConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            local_port=8848,
            secret_key="stcp-secret",
        )
    )

    assert 'serverAddr = "relay.example.test"' in rendered
    assert "serverPort = 443" in rendered
    assert 'transport.protocol = "wss"' in rendered
    assert 'type = "stcp"' in rendered


def test_render_frpc_visitor_config_uses_stcp_visitor() -> None:
    rendered = render_frpc_visitor_config(
        FrpcVisitorConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            server_name="cluster-relay",
            visitor_name="desktop-relay",
            bind_port=8765,
            secret_key="stcp-secret",
        )
    )

    assert 'serverAddr = "relay.example.test"' in rendered
    assert "serverPort = 443" in rendered
    assert 'transport.protocol = "wss"' in rendered
    assert "[[visitors]]" in rendered
    assert 'type = "stcp"' in rendered
    assert 'serverName = "cluster-relay"' in rendered
    assert 'bindAddr = "127.0.0.1"' in rendered
    assert "bindPort = 8765" in rendered


def test_render_frpc_config_supports_xtcp_proxy_and_visitor() -> None:
    proxy = render_frpc_config(
        FrpcConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            proxy_name="cluster-direct",
            proxy_type="xtcp",
            local_port=8848,
            secret_key="xtcp-secret",
        )
    )
    visitor = render_frpc_visitor_config(
        FrpcVisitorConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            visitor_name="desktop-direct",
            visitor_type="xtcp",
            server_name="cluster-direct",
            bind_port=8765,
            secret_key="xtcp-secret",
            keep_tunnel_open=True,
        )
    )

    assert 'type = "xtcp"' in proxy
    assert 'name = "cluster-direct"' in proxy
    assert 'type = "xtcp"' in visitor
    assert 'serverName = "cluster-direct"' in visitor
    assert "keepTunnelOpen = true" in visitor


def test_live_doctor_requires_frps_address(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")

    with pytest.raises(ConfigurationError, match="CLIO_RELAY_FRPS_ADDR"):
        run_doctor(settings, live=True)


def test_live_doctor_accepts_cluster_frps_address(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frp_token="secret",
        frpc_bin="python",
    )

    lines = run_doctor(settings, live=True, frps_addr="frps.example.test")

    assert "frps_addr: frps.example.test" in lines
    assert "frp_token: configured" in lines
    assert any(line.startswith("frpc:") for line in lines)


def test_live_doctor_reports_missing_frp_token(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="python",
    )

    lines = run_doctor(settings, live=True, frps_addr="frps.example.test")

    assert "frp_token: missing" in lines


def test_live_doctor_does_not_require_cluster_tools_locally(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frps_addr="frps.example.test",
        frp_token="secret",
        frpc_bin="python",
        jarvis_bin="definitely-not-local-jarvis",
        agent_bin="definitely-not-local-agent",
    )

    lines = run_doctor(settings, live=True)

    assert "frps_addr: frps.example.test" in lines
    assert "frp_token: configured" in lines
    assert any(line.startswith("frpc:") for line in lines)


def test_endpoint_user_service_is_sudo_less_and_configured() -> None:
    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(
            name="test-cluster",
            ssh_host="test-host",
            agent_adapter="exec",
            agent_npm_bin="current-agent",
            agent_args=["--prompt", "{prompt_path}"],
        ),
    )

    assert (
        "ExecStartPre=%h/.local/bin/clio-relay queue migrate-indexes --all --batch-size 500"
    ) in rendered
    assert (
        "ExecStart=%h/.local/bin/clio-relay endpoint start --role worker --cluster test-cluster"
    ) in rendered
    assert 'Environment="CLIO_RELAY_CORE_DIR=%h/.local/share/clio-relay/core"' in rendered
    assert 'Environment="CLIO_RELAY_AGENT_BIN=%h/.local/bin/current-agent"' in rendered
    assert 'Environment="CLIO_RELAY_AGENT_ADAPTER=exec"' in rendered
    assert (
        'Environment="CLIO_RELAY_INSTALL_RECEIPT=%h/.local/share/clio-relay/install-receipt.json"'
    ) in rendered
    assert "sudo" not in rendered


def test_endpoint_user_service_uses_cluster_executable_overrides() -> None:
    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(
            name="test-cluster",
            ssh_host="test-host",
            jarvis_bin="/opt/jarvis/current",
            spack_executable="/opt/site/spack/bin/spack",
            frpc_bin="/opt/frp/frpc",
            agent_bin="/opt/agents/clio",
        ),
    )

    assert 'Environment="CLIO_RELAY_JARVIS_BIN=/opt/jarvis/current"' in rendered
    assert 'Environment="JARVIS_MCP_SPACK_COMMAND=/opt/site/spack/bin/spack"' in rendered
    assert 'Environment="CLIO_RELAY_FRPC_BIN=/opt/frp/frpc"' in rendered
    assert 'Environment="CLIO_RELAY_AGENT_BIN=/opt/agents/clio"' in rendered
    assert "UnsetEnvironment=JARVIS_MCP_SPACK_COMMAND" not in rendered


def test_endpoint_user_service_unsets_absent_optional_manager_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stale systemd-manager overrides cannot leak into a generated worker unit."""
    monkeypatch.delenv("CLIO_RELAY_JARVIS_MCP_COMMAND", raising=False)

    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
    )

    assert "UnsetEnvironment=CLIO_RELAY_JARVIS_MCP_COMMAND" in rendered
    assert "UnsetEnvironment=JARVIS_MCP_SPACK_COMMAND" in rendered


def test_endpoint_user_service_passes_optional_jarvis_mcp_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "CLIO_RELAY_JARVIS_MCP_COMMAND",
        '["uvx","--from","git+https://github.com/iowarp/clio-kit.git@branch","clio-kit"]',
    )

    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
    )

    assert (
        'Environment="CLIO_RELAY_JARVIS_MCP_COMMAND=[\\"uvx\\",\\"--from\\",'
        '\\"git+https://github.com/iowarp/clio-kit.git@branch\\",\\"clio-kit\\"]"'
    ) in rendered
    assert "UnsetEnvironment=CLIO_RELAY_JARVIS_MCP_COMMAND" not in rendered


def test_endpoint_user_service_escapes_arbitrary_labels_and_values() -> None:
    """Systemd rendering cannot turn operator values into directives or unit paths."""
    rendered = render_endpoint_user_service(
        cluster='Target GPU %n "quoted"\nExecStart=/bin/false',
        definition=ClusterDefinition(
            name="Target GPU",
            ssh_host="target-gpu",
            agent_bin='/opt/agent "current" %n',
        ),
    )

    assert rendered.count("\nExecStart=") == 1
    assert rendered.count("\nEnvironment=") == 9
    assert "\\nExecStart=/bin/false" in rendered
    assert "%%n" in rendered
    assert 'CLIO_RELAY_AGENT_BIN=/opt/agent \\"current\\" %%n' in rendered


def test_endpoint_service_install_uses_safe_unit_name_and_bounded_ssh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        observed.update(command=command, input=input, timeout=timeout)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b"installed\n", b"")

    monkeypatch.setattr(deployment.subprocess, "run", run)

    lines = install_endpoint_user_service_over_ssh(
        cluster='Target GPU %n "quoted"',
        ssh_host="target-gpu",
        service_text="[Service]\nExecStart=/bin/true\n",
        start=False,
        enable=False,
        timeout_seconds=15,
    )

    assert lines == ["installed"]
    assert observed["command"] == ["ssh", "target-gpu", "bash", "-s"]
    assert observed["timeout"] == 15
    script = cast(bytes, observed["input"]).decode("utf-8")
    assert "clio-relay-worker-k2-" in script
    assert "Target GPU" not in script


def test_endpoint_service_install_requires_linger_before_any_mutation() -> None:
    """A persistent install cannot write or control a unit under a login-scoped manager."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the remote endpoint installer")
    script = deployment._remote_install_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        service_name="clio-relay-worker-test.service",
        service_text="[Service]\nExecStart=/bin/true\n",
        start=True,
        enable=True,
        require_persistent=True,
    )
    harness = f"""set -u
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
export HOME="$test_root/home" USER=test-user
loginctl() {{ echo no; }}
systemctl() {{ echo "unexpected-systemctl=$*" >&2; return 99; }}
{script}
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=10,
    )
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 78
    assert "persistent endpoint service requires systemd user lingering" in stderr
    assert "loginctl enable-linger test-user" in stderr
    assert "unexpected-systemctl" not in stderr
    assert script.index('if [ "$linger" = "yes" ]') < script.index('mkdir -p "$HOME')


@pytest.mark.parametrize(
    ("linger", "require_persistent", "expected_mode", "expected_warning"),
    [
        ("yes", True, "systemd-user-linger", None),
        (
            "no",
            False,
            "login-scoped",
            "endpoint service is login-scoped and may stop after the final login exits",
        ),
    ],
)
def test_endpoint_service_install_verifies_enabled_active_and_persistence_mode(
    linger: str,
    require_persistent: bool,
    expected_mode: str,
    expected_warning: str | None,
) -> None:
    """Successful installs report exact persistence, enabled, and active states."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the remote endpoint installer")
    script = deployment._remote_install_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        service_name="clio-relay-worker-test.service",
        service_text="[Service]\nExecStart=/bin/true\n",
        start=True,
        enable=True,
        require_persistent=require_persistent,
    )
    harness = f"""set -u
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
export HOME="$test_root/home" USER=test-user FAKE_LINGER={linger}
loginctl() {{ echo "$FAKE_LINGER"; }}
systemctl() {{
  echo "systemctl=$*" >&2
  case "${{2:-}}" in
    is-enabled) echo enabled ;;
    is-active) echo active ;;
    is-system-running) echo running ;;
  esac
}}
{script}
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=10,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert f"linger={linger}" in stdout
    assert f"endpoint_service.persistence={expected_mode}" in stdout
    assert "endpoint_service.enabled=enabled" in stdout
    assert "endpoint_service.active=active" in stdout
    assert "systemctl=--user daemon-reload" in stderr
    assert "systemctl=--user enable clio-relay-worker-test.service" in stderr
    assert "systemctl=--user restart clio-relay-worker-test.service" in stderr
    if expected_warning is None:
        assert "login-scoped" not in stderr
    else:
        assert expected_warning in stderr


@pytest.mark.parametrize(
    ("enabled_state", "active_state", "expected_error"),
    [
        ("disabled", "active", "endpoint service is not enabled"),
        ("enabled", "inactive", "endpoint service is not active"),
    ],
)
def test_endpoint_service_install_rejects_unverified_service_state(
    enabled_state: str,
    active_state: str,
    expected_error: str,
) -> None:
    """A requested enabled/running deployment fails when systemd disproves either state."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the remote endpoint installer")
    script = deployment._remote_install_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        service_name="clio-relay-worker-test.service",
        service_text="[Service]\nExecStart=/bin/true\n",
        start=True,
        enable=True,
        require_persistent=True,
    )
    harness = f"""set -u
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
export HOME="$test_root/home" USER=test-user
loginctl() {{ echo yes; }}
systemctl() {{
  case "${{2:-}}" in
    is-enabled) echo {enabled_state} ;;
    is-active) echo {active_state} ;;
    is-system-running) echo running ;;
  esac
}}
{script}
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 1
    assert expected_error in result.stderr.decode("utf-8", errors="replace")


def test_endpoint_service_install_rejects_unsafe_or_unbounded_ssh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal called
        called = True
        raise AssertionError("unsafe destination must fail before SSH")

    monkeypatch.setattr(deployment.subprocess, "run", run)
    with pytest.raises(RelayError, match="non-option destination"):
        install_endpoint_user_service_over_ssh(
            cluster="target",
            ssh_host="-oProxyCommand=evil",
            service_text="[Service]\nExecStart=/bin/true\n",
            start=False,
            enable=False,
        )
    with pytest.raises(RelayError, match="finite and positive"):
        install_endpoint_user_service_over_ssh(
            cluster="target",
            ssh_host="target",
            service_text="[Service]\nExecStart=/bin/true\n",
            start=False,
            enable=False,
            timeout_seconds=float("nan"),
        )
    assert called is False


def test_endpoint_service_install_reports_ssh_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def timeout(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(cmd=["ssh"], timeout=2)

    monkeypatch.setattr(deployment.subprocess, "run", timeout)

    with pytest.raises(RelayError, match="exceeded 2 seconds"):
        install_endpoint_user_service_over_ssh(
            cluster="target",
            ssh_host="target",
            service_text="[Service]\nExecStart=/bin/true\n",
            start=False,
            enable=False,
            timeout_seconds=2,
        )

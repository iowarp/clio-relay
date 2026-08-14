"""Tests for the frp process substrate (#231 R4).

No test here spawns a real ``frpc``: every :class:`HeldFrpVisitor` in this
file is given a fake ``process_factory`` (mirroring the injection seam
``tests/test_transport_probe.py`` already relies on for the remote-side
probe processes -- zero ``Popen`` monkeypatches, only factory injection).
"""

from __future__ import annotations

import os
import stat
import subprocess
from io import BytesIO
from typing import IO, Any

import pytest
from pytest import MonkeyPatch

import clio_relay.frp_link as frp_link
import clio_relay.transport_probe as transport_probe
from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_link import (
    DEFAULT_STDERR_BUFFER_MAX_BYTES,
    FrpLinkConfig,
    HeldFrpVisitor,
    render_visitor_config,
    require_frp_server_addr,
)
from clio_relay.relay_host import FrpTransportProtocol


def _link_config(**overrides: Any) -> FrpLinkConfig:
    values: dict[str, Any] = {
        "server_addr": "relay.example.test",
        "server_port": 443,
        "protocol": FrpTransportProtocol.WSS,
        "token": "frp-token",
        "secret_key": "stcp-secret",
        "proxy_name": "relay-http",
    }
    values.update(overrides)
    return FrpLinkConfig(**values)


class FakeFrpProcess:
    """A visible, in-memory stand-in for a spawned ``frpc`` process."""

    def __init__(
        self,
        command: list[str],
        *,
        ignores_terminate: bool = False,
        dead_on_arrival: bool = False,
        stderr: bytes = b"",
    ) -> None:
        self.command = command
        # Declared as IO[bytes] (not the inferred BytesIO) to match
        # FrpProcess's protocol attributes exactly: Protocol attributes are
        # checked invariantly, so a narrower declared type would make this
        # fake structurally incompatible.
        # Always a real, writable buffer (never None): the fake stands in for
        # both the remote ssh probe process (which writes+closes stdin) and
        # the local frpc visitor (which never touches it) in
        # test_probe_and_transport_share_one_visitor_implementation.
        self.stdin: IO[bytes] | None = BytesIO()
        self.stdout: IO[bytes] | None = BytesIO()
        self.stderr: IO[bytes] | None = BytesIO(stderr)
        self.returncode: int | None = 1 if dead_on_arrival else None
        self.terminate_calls = 0
        self.kill_calls = 0
        self._ignores_terminate = ignores_terminate

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminate_calls += 1
        if not self._ignores_terminate:
            self.returncode = 0

    def kill(self) -> None:
        self.kill_calls += 1
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        if self.returncode is None:
            raise subprocess.TimeoutExpired(cmd=self.command, timeout=timeout or 0)
        return self.returncode


# --------------------------------------------------------------------------
# Renderer golden tests
# --------------------------------------------------------------------------


def test_render_visitor_config_stcp_golden() -> None:
    rendered = render_visitor_config(_link_config(), local_bind_port=9876)

    assert 'serverAddr = "relay.example.test"' in rendered
    assert "serverPort = 443" in rendered
    assert 'auth.token = "frp-token"' in rendered
    assert 'transport.protocol = "wss"' in rendered
    assert "[[visitors]]" in rendered
    assert 'name = "relay-http-visitor"' in rendered
    assert 'type = "stcp"' in rendered
    assert 'serverName = "relay-http"' in rendered
    assert 'secretKey = "stcp-secret"' in rendered
    assert "bindPort = 9876" in rendered
    assert "keepTunnelOpen" not in rendered


def test_render_visitor_config_xtcp_sets_keep_tunnel_open() -> None:
    rendered = render_visitor_config(
        _link_config(proxy_name="relay-http-direct"),
        local_bind_port=9877,
        visitor_type="xtcp",
        keep_tunnel_open=True,
    )

    assert 'type = "xtcp"' in rendered
    assert "keepTunnelOpen = true" in rendered
    assert 'name = "relay-http-direct-visitor"' in rendered


def test_render_visitor_config_rejects_nonpositive_bind_port() -> None:
    with pytest.raises(ConfigurationError, match="local_bind_port"):
        render_visitor_config(_link_config(), local_bind_port=0)


# --------------------------------------------------------------------------
# FrpLinkConfig.from_cluster: env-bound secrets, never a literal
# --------------------------------------------------------------------------


def test_from_cluster_reads_token_and_secret_from_declared_env_names() -> None:
    definition = ClusterDefinition(
        name="test-cluster",
        ssh_host="test-host",
        frp_transport=FrpTransportConfig(
            server_addr="relay.example.test",
            token_env="CUSTOM_FRP_TOKEN_NAME",
            stcp_secret_env="CUSTOM_STCP_SECRET_NAME",
        ),
    )
    env = {
        "CUSTOM_FRP_TOKEN_NAME": "env-supplied-token-92db",
        "CUSTOM_STCP_SECRET_NAME": "env-supplied-secret-6f1a",
        # A same-shaped default-named var is deliberately present with a
        # DIFFERENT value, proving resolution follows the cluster's
        # *declared* binding name, not a hardcoded default name.
        "CLIO_RELAY_FRP_TOKEN": "wrong-default-name-value",
        "CLIO_RELAY_STCP_SECRET": "wrong-default-name-value",
    }

    config = FrpLinkConfig.from_cluster(
        definition,
        cluster="test-cluster",
        proxy_name="relay-http",
        env=env,
    )

    assert config.token == "env-supplied-token-92db"
    assert config.secret_key == "env-supplied-secret-6f1a"
    assert config.server_addr == "relay.example.test"


def test_from_cluster_refuses_when_declared_secret_env_is_unset() -> None:
    definition = ClusterDefinition(
        name="test-cluster",
        ssh_host="test-host",
        frp_transport=FrpTransportConfig(server_addr="relay.example.test"),
    )
    # The declared binding (CLIO_RELAY_STCP_SECRET, the default) is absent
    # from this environment -- no literal fallback, no silent empty secret.
    env = {"CLIO_RELAY_FRP_TOKEN": "frp-token"}

    with pytest.raises(ConfigurationError, match="CLIO_RELAY_STCP_SECRET"):
        FrpLinkConfig.from_cluster(
            definition,
            cluster="test-cluster",
            proxy_name="relay-http",
            env=env,
        )


def test_from_cluster_refuses_when_server_addr_is_blank() -> None:
    definition = ClusterDefinition(name="test-cluster", ssh_host="test-host")
    env = {"CLIO_RELAY_FRP_TOKEN": "t", "CLIO_RELAY_STCP_SECRET": "s"}

    with pytest.raises(ConfigurationError, match="frp server address is not configured"):
        FrpLinkConfig.from_cluster(
            definition,
            cluster="test-cluster",
            proxy_name="relay-http",
            env=env,
        )


def test_render_visitor_config_secret_comes_from_resolved_env_value() -> None:
    """A resolved-from-env config's TOML carries the env value, never a default."""
    definition = ClusterDefinition(
        name="test-cluster",
        ssh_host="test-host",
        frp_transport=FrpTransportConfig(server_addr="relay.example.test"),
    )
    env = {
        "CLIO_RELAY_FRP_TOKEN": "env-token-abc123",
        "CLIO_RELAY_STCP_SECRET": "env-secret-xyz789",
    }
    config = FrpLinkConfig.from_cluster(
        definition,
        cluster="test-cluster",
        proxy_name="relay-http",
        env=env,
    )

    rendered = render_visitor_config(config, local_bind_port=9876)

    assert 'auth.token = "env-token-abc123"' in rendered
    assert 'secretKey = "env-secret-xyz789"' in rendered


def test_require_frp_server_addr_names_the_cluster() -> None:
    with pytest.raises(ConfigurationError, match="not configured for cluster test-cluster"):
        require_frp_server_addr("", "test-cluster")
    assert require_frp_server_addr("relay.example.test", "test-cluster") == "relay.example.test"


# --------------------------------------------------------------------------
# HeldFrpVisitor lifecycle
# --------------------------------------------------------------------------


def test_visitor_config_is_written_0600_and_removed_on_close() -> None:
    processes: list[FakeFrpProcess] = []

    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        process = FakeFrpProcess(command)
        processes.append(process)
        return process

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19876,
        process_factory=factory,
    )
    visitor.establish()

    config_path = visitor.config_path
    assert config_path is not None
    assert config_path.exists()
    assert config_path.name == "frpc-visitor.toml"
    if os.name != "nt":
        assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
    assert processes[0].command == ["frpc", "-c", str(config_path)]
    assert visitor.is_alive()

    visitor.close()

    assert not config_path.exists()
    assert not config_path.parent.exists()
    assert not visitor.is_alive()


def test_a_visitor_that_ignores_terminate_is_killed() -> None:
    processes: list[FakeFrpProcess] = []

    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        process = FakeFrpProcess(command, ignores_terminate=True)
        processes.append(process)
        return process

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19877,
        process_factory=factory,
    )
    visitor.establish()
    assert visitor.is_alive()

    visitor.close()

    assert processes[0].terminate_calls == 1
    assert processes[0].kill_calls == 1
    assert not visitor.is_alive()


def test_bring_up_failure_carries_bounded_stderr_not_raw_output() -> None:
    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(
            command,
            dead_on_arrival=True,
            stderr=b"bind: address already in use\n",
        )

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19878,
        process_factory=factory,
    )
    visitor.establish()

    assert not visitor.is_alive()
    assert visitor.failure_detail() == "bind: address already in use"


def test_bring_up_failure_bounds_a_large_stderr() -> None:
    huge = (b"x" * 5_000) + b"\n"

    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(command, dead_on_arrival=True, stderr=huge)

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19879,
        process_factory=factory,
    )
    visitor.establish()

    detail = visitor.failure_detail()
    assert detail is not None
    assert len(detail) <= DEFAULT_STDERR_BUFFER_MAX_BYTES
    assert len(huge) > DEFAULT_STDERR_BUFFER_MAX_BYTES


def test_establish_may_only_be_called_once() -> None:
    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(command)

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19880,
        process_factory=factory,
    )
    visitor.establish()

    with pytest.raises(RelayError, match="already established"):
        visitor.establish()


def test_held_frp_visitor_rejects_nonpositive_bind_port() -> None:
    with pytest.raises(ConfigurationError, match="local_bind_port"):
        HeldFrpVisitor(frpc_bin="frpc", config=_link_config(), local_bind_port=0)


# --------------------------------------------------------------------------
# wait_for_channel_health: the promoted, mode-agnostic health-wait
# --------------------------------------------------------------------------


def test_wait_for_channel_health_raises_when_process_exits_during_bring_up() -> None:
    process = FakeFrpProcess(["frpc"], dead_on_arrival=True)

    # timeout_seconds is generous (not the ~50ms poll interval) because
    # constructing httpx.Client() itself can eat a nontrivial slice of a very
    # small budget on some hosts; a too-tight timeout here would make this
    # assert the generic "did not become ready" fallback instead of the
    # poll()-detected exit this test targets.
    with pytest.raises(RelayError, match="exited during bring-up"):
        frp_link.wait_for_channel_health(
            process,
            base_url="http://127.0.0.1:1",
            timeout_seconds=5.0,
        )


# --------------------------------------------------------------------------
# Proof of delegation: transport_probe.py's local visitor IS frp_link's
# --------------------------------------------------------------------------


def test_probe_and_transport_share_one_visitor_implementation(monkeypatch: MonkeyPatch) -> None:
    """``run_frp_http_probe`` constructs and drives a REAL ``HeldFrpVisitor``.

    Patches ``clio_relay.transport_probe.HeldFrpVisitor`` -- the name the
    probe actually looks up at call time (``transport_probe.py`` imports the
    bare symbol from ``frp_link``, the same "patched where it's looked up"
    pattern ``tests/test_transport_probe.py`` already relies on for
    ``_wait_for_healthz``/``_cleanup_remote_probe``) -- with a subclass of
    the real :class:`~clio_relay.frp_link.HeldFrpVisitor` that records every
    lifecycle call before delegating to the real implementation. If the probe
    ever stopped delegating to ``frp_link`` (reimplementing the visitor
    inline again), this call log would go empty.
    """
    calls: list[str] = []
    real_cls = frp_link.HeldFrpVisitor

    class RecordingHeldFrpVisitor(real_cls):  # type: ignore[misc, valid-type]
        def establish(self) -> None:
            calls.append("establish")
            super().establish()

        def is_alive(self) -> bool:
            calls.append("is_alive")
            return super().is_alive()

        def close(self) -> None:
            calls.append("close")
            super().close()

    monkeypatch.setattr(transport_probe, "HeldFrpVisitor", RecordingHeldFrpVisitor)

    processes: list[FakeFrpProcess] = []

    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        process = FakeFrpProcess(command)
        processes.append(process)
        return process

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        del timeout_seconds

    def fake_cleanup(**_kwargs: object) -> list[str]:
        return ["transport.remote_cleanup=passed"]

    monkeypatch.setattr(transport_probe, "_wait_for_healthz", fake_healthz)
    monkeypatch.setattr(transport_probe, "_cleanup_remote_probe", fake_cleanup)

    lines = transport_probe.run_frp_http_probe(
        cluster="test-cluster",
        definition=ClusterDefinition(
            name="test-cluster",
            ssh_host="test-host",
            frp_transport=FrpTransportConfig(server_addr="relay.example.test"),
        ),
        frpc_bin="frpc",
        token="frp-token",
        secret_key="stcp-secret",
        local_bind_port=19881,
        api_token="api-token",
        process_factory=fake_process_factory,
    )

    assert lines[-1] == "transport.cleanup=passed"
    assert "establish" in calls
    assert "close" in calls
    assert processes[1].command[0] == "frpc"

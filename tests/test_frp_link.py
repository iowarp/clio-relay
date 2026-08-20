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
import tempfile
import threading
from contextlib import suppress
from io import BytesIO
from typing import IO, Any

import pytest
from pytest import MonkeyPatch

import clio_relay.control_channel as control_channel
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
        stdout: bytes = b"",
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
        self.stdout: IO[bytes] | None = BytesIO(stdout)
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
    assert visitor.failure_detail() == "stderr: bind: address already in use"


def test_bring_up_failure_carries_bounded_stdout_too() -> None:
    """#231 R4 opus review F1/F2: frpc logs to stdout by default -- it must be
    drained (never wedging the child) and reachable in failure_detail(), not
    silently dropped in favor of stderr alone.
    """

    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(
            command,
            dead_on_arrival=True,
            stdout=b"login to server success\nbind: address already in use\n",
        )

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19878,
        process_factory=factory,
    )
    visitor.establish()

    assert not visitor.is_alive()
    detail = visitor.failure_detail()
    assert detail is not None
    assert detail.startswith("stdout: ")
    assert "address already in use" in detail


def test_bring_up_failure_labels_both_streams_when_both_have_content() -> None:
    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(
            command,
            dead_on_arrival=True,
            stdout=b"stdout-diagnostic\n",
            stderr=b"stderr-diagnostic\n",
        )

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19878,
        process_factory=factory,
    )
    visitor.establish()

    detail = visitor.failure_detail()
    assert detail == "stdout: stdout-diagnostic\nstderr: stderr-diagnostic"


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
    # "stderr: " (8 chars) prefixes the bounded buffer's own content, so the
    # buffer's bound, not the labeled string, is what's asserted here.
    assert len(detail) <= DEFAULT_STDERR_BUFFER_MAX_BYTES + len("stderr: ")
    assert len(huge) > DEFAULT_STDERR_BUFFER_MAX_BYTES


def test_stdout_pipe_is_drained_so_a_chatty_child_never_wedges() -> None:
    """#231 R4 opus review F1 (the blocker): an unread stdout pipe wedges the
    child once its OS pipe buffer fills, invisibly (is_alive() stays True,
    failure_detail() stays None). Writes more than a small pipe's buffer
    (~4-64 KiB depending on platform) from a real OS pipe and asserts the
    write completes -- i.e. never blocks -- because HeldFrpVisitor is
    continuously draining the read end in a background thread.
    """
    read_fd, write_fd = os.pipe()
    read_stream = os.fdopen(read_fd, "rb")
    write_stream = os.fdopen(write_fd, "wb")

    class _PipedProcess:
        def __init__(self, command: list[str]) -> None:
            self.command = command
            self.stdin: IO[bytes] | None = None
            self.stdout: IO[bytes] | None = read_stream
            self.stderr: IO[bytes] | None = BytesIO()
            self.returncode: int | None = None

        def poll(self) -> int | None:
            return self.returncode

        def terminate(self) -> None:
            self.returncode = 0

        def kill(self) -> None:
            self.returncode = -9

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            return self.returncode or 0

    def factory(command: list[str], **_kwargs: Any) -> _PipedProcess:
        return _PipedProcess(command)

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19878,
        process_factory=factory,
    )
    visitor.establish()

    payload = (b"x" * 65_536) + b"post-write-sentinel\n"
    write_completed = threading.Event()

    def write_child_output() -> None:
        write_stream.write(payload)
        write_stream.flush()
        write_completed.set()
        write_stream.close()

    writer = threading.Thread(target=write_child_output, daemon=True)
    writer.start()
    try:
        writer.join(timeout=10)
        assert write_completed.is_set(), (
            "writing more than a pipe buffer's worth of stdout blocked -- "
            "the read end was never drained (the exact F1 regression)"
        )
        detail = visitor.failure_detail()
        assert detail is not None
        assert "post-write-sentinel" in detail
    finally:
        visitor.close()
        with suppress(OSError):
            read_stream.close()


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
# close(): a secret-bearing config file must never leak silently (F3)
# --------------------------------------------------------------------------


def test_close_reports_residual_config_file_when_cleanup_fails(monkeypatch: MonkeyPatch) -> None:
    """#231 R4 opus review F3: a failed temp-dir cleanup must be typed, not
    swallowed -- the rendered config carries a plaintext token/secret.

    Sabotage: monkeypatches ``TemporaryDirectory.cleanup`` to always raise
    (deterministic and cross-platform, unlike relying on OS file-locking
    quirks), proving the ``OSError`` from ``close()`` is caught and recorded
    rather than suppressed.
    """

    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(command)

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19883,
        process_factory=factory,
    )
    visitor.establish()
    config_path = visitor.config_path
    assert config_path is not None
    assert visitor.config_cleanup_error is None

    def raising_cleanup(self: tempfile.TemporaryDirectory[str]) -> None:
        del self
        raise OSError("simulated: the config file is still held open")

    monkeypatch.setattr(tempfile.TemporaryDirectory, "cleanup", raising_cleanup)

    visitor.close()

    assert visitor.config_cleanup_error is not None
    assert str(config_path) in visitor.config_cleanup_error
    assert "token/secret" in visitor.config_cleanup_error
    # The path is deliberately NOT nulled on a failed cleanup: a caller (the
    # transport_probe.py cleanup ledger) needs it to report exactly which
    # file is residual.
    assert visitor.config_path == config_path


@pytest.mark.skipif(
    os.name != "nt",
    reason="reproduces the review's exact Windows open-file-handle scenario",
)
def test_close_reports_residual_config_file_when_windows_holds_it_open() -> None:
    """The same F3 fix, reproduced without a monkeypatch: an actual open file
    handle on Windows makes ``shutil.rmtree`` (inside ``TemporaryDirectory.
    cleanup``) fail with a real ``OSError``, exactly as the review reproduced
    it (secret readable on disk, ledger said residual=False, pre-fix).
    """

    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(command)

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19884,
        process_factory=factory,
    )
    visitor.establish()
    config_path = visitor.config_path
    assert config_path is not None

    handle = open(config_path, "rb")  # noqa: SIM115 -- deliberately held open across close()
    try:
        visitor.close()
    finally:
        handle.close()

    try:
        assert visitor.config_cleanup_error is not None
        assert config_path.exists()
    finally:
        # Manual cleanup: close() left it behind on purpose (F3); do not
        # leak a real temp directory on the test host.
        with suppress(OSError):
            config_path.unlink()
        with suppress(OSError):
            config_path.parent.rmdir()


# --------------------------------------------------------------------------
# wait_for_channel_health / wait_healthy: parameterized subject (F4)
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


def test_wait_for_channel_health_default_subject_matches_pre_promotion_text() -> None:
    """The default ``subject`` reproduces SshForwardTransport's exact
    pre-promotion message byte-for-byte (#231 R4 opus review F4)."""
    process = FakeFrpProcess(["ssh"], dead_on_arrival=True)

    expected = (
        r"^owned session channel exited during bring-up: "
        r"channel forward did not become ready$"
    )
    with pytest.raises(RelayError, match=expected):
        frp_link.wait_for_channel_health(
            process,
            base_url="http://127.0.0.1:1",
            timeout_seconds=5.0,
        )


def test_wait_healthy_default_subject_names_the_visitor_type() -> None:
    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(command, dead_on_arrival=True)

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19885,
        visitor_type="xtcp",
        process_factory=factory,
    )
    visitor.establish()

    with pytest.raises(RelayError, match=r"^frp xtcp visitor exited during bring-up"):
        visitor.wait_healthy(timeout_seconds=5.0)


def test_wait_healthy_accepts_an_explicit_subject_override() -> None:
    """R5's transports pass their own label (e.g. "frp stcp link") rather
    than the process-centric default."""

    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(command, dead_on_arrival=True)

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19886,
        process_factory=factory,
    )
    visitor.establish()

    with pytest.raises(RelayError, match=r"^frp stcp link exited during bring-up"):
        visitor.wait_healthy(timeout_seconds=5.0, subject="frp stcp link")


# --------------------------------------------------------------------------
# pump_stderr thread naming: neutral by default, never "frp"-branded
# for ssh_forward (F5)
# --------------------------------------------------------------------------


def test_pump_stderr_default_thread_name_is_neutral_not_frp_branded() -> None:
    buffer = frp_link.BoundedStderrBuffer()
    stream = BytesIO(b"")

    thread = frp_link.pump_stderr(stream, buffer)
    try:
        assert thread.name == "clio-relay-held-stderr"
        assert "frp" not in thread.name
    finally:
        thread.join(timeout=2.0)


def test_held_frp_visitor_names_its_stdout_and_stderr_pump_threads() -> None:
    def factory(command: list[str], **_kwargs: Any) -> FakeFrpProcess:
        return FakeFrpProcess(command)

    visitor = HeldFrpVisitor(
        frpc_bin="frpc",
        config=_link_config(),
        local_bind_port=19887,
        process_factory=factory,
    )
    visitor.establish()
    try:
        stdout_thread = visitor._stdout_thread  # pyright: ignore[reportPrivateUsage]
        stderr_thread = visitor._stderr_thread  # pyright: ignore[reportPrivateUsage]
        assert stdout_thread is not None
        assert stdout_thread.name == "clio-relay-frp-stdout"
        assert stderr_thread is not None
        assert stderr_thread.name == "clio-relay-frp-stderr"
    finally:
        visitor.close()


class _FakeSshChannelProcess:
    """A minimal fake for SshForwardTransport's held process.

    Its stdout is deliberately empty: this test only needs establish() to
    reach the pump_stderr call site (before spawning any thread that reads
    stdout), not to complete a real bring-up -- _read_bootstrap failing
    immediately afterward on empty/EOF stdout is expected and irrelevant to
    what this test asserts.
    """

    def __init__(self) -> None:
        self.stdin: IO[bytes] | None = BytesIO()
        self.stdout: IO[bytes] | None = BytesIO(b"")
        self.stderr: IO[bytes] | None = BytesIO(b"")
        self.returncode: int | None = None

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = 0

    def kill(self) -> None:
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return self.returncode or 0


def test_ssh_forward_stderr_pump_thread_is_not_frp_branded(monkeypatch: MonkeyPatch) -> None:
    """The exact regression F5 caught: promoting pump_stderr's implementation
    out of control_channel.py must not also promote a "frp"-branded default
    onto the ssh_forward thread it spawns. Exercises the REAL
    SshForwardTransport.establish() -- not just frp_link.pump_stderr's own
    default -- since the regression was specifically in control_channel.py's
    call site losing its explicit override during the R4 promotion.
    """
    calls: list[str] = []
    real_pump_stderr = control_channel.pump_stderr

    def recording_pump_stderr(
        stream: IO[bytes],
        buffer: control_channel.BoundedStderrBuffer,
        *,
        thread_name: str = "clio-relay-held-stderr",
    ) -> threading.Thread:
        calls.append(thread_name)
        return real_pump_stderr(stream, buffer, thread_name=thread_name)

    monkeypatch.setattr(control_channel, "pump_stderr", recording_pump_stderr)

    def factory(*_args: Any, **_kwargs: Any) -> _FakeSshChannelProcess:
        return _FakeSshChannelProcess()

    transport = control_channel.SshForwardTransport(
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        session_id="session-1",
        session_generation_id="generation-1",
        remote_api_port=8765,
        bootstrap_script="echo hi",
        process_factory=factory,
        ready_timeout_seconds=0.3,
    )
    with pytest.raises(Exception):  # noqa: B017 -- bootstrap read failure, not this test's target
        transport.establish(nonce="1" * 64)

    assert calls == ["clio-relay-channel-stderr"]
    assert "frp" not in calls[0]


# --------------------------------------------------------------------------
# The stderr-buffer bound is pinned equal to control_channel's event-detail
# bound (F6): not derived from one another, but a coincidence this test
# makes a conscious decision instead of silent drift.
# --------------------------------------------------------------------------


def test_stderr_buffer_bound_matches_channel_event_detail_bound() -> None:
    assert (
        frp_link.DEFAULT_STDERR_BUFFER_MAX_BYTES == control_channel.MAX_CHANNEL_EVENT_DETAIL_CHARS
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

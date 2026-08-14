"""The one-held-tunnel invariant for modes (a)/(b) (#231 R5, #188 items 1-3+5).

Cloned from ``tests/test_owned_session_channel.py``'s harness: a fake process
factory records every dial, keyed on argv so an ``frpc`` spawn (one held frp
visitor "pair") and an ``ssh`` spawn (one dial) are counted separately. The
headline assertion throughout is ``ssh_dials == 0`` -- these modes must never
fall back to, or accidentally reach, an ssh dial. No real ``frpc``, no real
cluster: only a temp dir for the rendered visitor TOML, which ``HeldFrpVisitor``
itself still writes for real, and (for exactly one health-timeout test that
deliberately does not mock ``wait_for_channel_health``) a real, immediately
-refused loopback connection attempt against a port nothing is listening on --
no other test in this file opens a real socket.
"""

from __future__ import annotations

import io
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay import frp_link
from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.config import RelaySettings, TransportMode
from clio_relay.control_channel import (
    ChannelBootstrapError,
    ChannelDropped,
    StreamChannelsUnavailable,
    TransportIdentityAnchorRequired,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_transport import BrokeredTcpTransport, TransportPunchFailed
from clio_relay.remote_connection import (
    DEFAULT_OWNED_SESSION_API_PORT,
    RemoteConnection,
    RemoteConnectionRegistry,
)
from clio_relay.session_api import (
    SESSION_IDENTITY_SCHEMA,
    OwnedSessionApiClient,
    session_identity_document,
)

NONCE = "1" * 64
OWNER_TOKEN = "owner-token"


class _Response:
    def __init__(self, document: object, *, status: int = 200) -> None:
        self._payload = json.dumps(document).encode("utf-8")
        self.status = status
        self.will_close = False

    def read(self, _amount: int) -> bytes:
        return self._payload


class _Socket:
    def __init__(self, timeout: float | None) -> None:
        self.timeout = timeout

    def gettimeout(self) -> float | None:
        return self.timeout

    def settimeout(self, timeout: float | None) -> None:
        self.timeout = timeout


class _Stream:
    """One fake HTTP connection over the held tunnel.

    Serves both the short-lived bring-up fetch (``frp_transport._get_bounded_json``,
    a new connection per GET) and the kept-alive pooled operation stream
    (``remote_connection``'s identity-bound stream) -- both go through the
    exact same ``http.client.HTTPConnection`` seam.
    """

    def __init__(self, harness: _Harness, timeout: float) -> None:
        self.harness = harness
        self.auto_open = 1
        self.timeout = timeout
        self.socket = _Socket(timeout)
        self.sock: object | None = None
        self.closed = False

    def connect(self) -> None:
        self.sock = self.socket

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.harness.requests.append(
            {"method": method, "path": path, "body": body, "headers": dict(headers or {})}
        )

    def getresponse(self) -> _Response:
        path = cast(str, self.harness.requests[-1]["path"])
        if path.startswith("/session-identity"):
            if self.harness.rogue_identity:
                # A rogue responder that does NOT know this connection's
                # pinned identity -- it can only guess, and guesses wrong.
                return _Response(
                    {
                        "schema_version": SESSION_IDENTITY_SCHEMA,
                        "cluster": "not-the-pinned-cluster",
                        "session_id": "not-the-pinned-session",
                        "session_generation_id": "not-the-pinned-generation",
                        "nonce": NONCE,
                        "hmac_sha256": "0" * 64,
                    }
                )
            return _Response(self.harness.identity)
        if path == "/session-status":
            return _Response(self.harness.status)
        return _Response(self.harness.responses.get(path, {"ok": True}))

    def close(self) -> None:
        self.closed = True
        self.sock = None


class _ManagedProcess:
    """One fake spawned process: exactly one of these is one dial or one frp pair."""

    def __init__(self, argv: list[str], *, kind: str) -> None:
        self.argv = argv
        self.kind = kind  # "ssh" or "frpc"
        self.stdin: io.BytesIO | None = None
        self.stdout: io.BytesIO | None = None
        self.stderr: io.BytesIO = io.BytesIO(b"")
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        if self.returncode is None:
            self.returncode = -15

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def drop(self) -> None:
        """Simulate the held visitor process dying while the connection holds it."""
        self.returncode = 255


class _Harness:
    """Records every dial, every rendered config, and every request made."""

    def __init__(self, *, cluster: str = "ares", generation_id: str = "generation-1") -> None:
        self.cluster = cluster
        self.generation_id = generation_id
        self.processes: list[_ManagedProcess] = []
        self.spawn_kwargs: list[dict[str, object]] = []
        self.visitor_configs: list[str] = []
        self.visitor_config_paths: list[Path] = []
        self.xtcp_should_fail = False
        # One-shot injections consumed by the NEXT frpc spawn only.
        self.next_frpc_stdout: bytes | None = None
        self.next_frpc_exit: bytes | None = None
        self.rogue_identity = False
        self.streams: list[_Stream] = []
        self.requests: list[dict[str, object]] = []
        self.responses: dict[str, object] = {}
        self.registry = RemoteConnectionRegistry()

    @property
    def ssh_dials(self) -> int:
        """Return the number of new SSH connections opened so far."""
        return sum(1 for process in self.processes if process.kind == "ssh")

    @property
    def frp_pairs(self) -> int:
        """Return the number of held frp visitor processes spawned so far."""
        return sum(1 for process in self.processes if process.kind == "frpc")

    @property
    def status(self) -> dict[str, object]:
        """Return the exact ``/session-status`` document the remote relay reports."""
        return {
            "owner": "clio-relay",
            "cluster": self.cluster,
            "session_id": "desktop-session-1",
            "session_generation_id": self.generation_id,
            "remote_api_port": DEFAULT_OWNED_SESSION_API_PORT,
            "running": True,
            "ownership_verified": True,
        }

    @property
    def identity(self) -> dict[str, str]:
        """Return the ``/session-identity`` document the remote relay signs."""
        return session_identity_document(
            owner_token=OWNER_TOKEN,
            cluster=self.cluster,
            session_id="desktop-session-1",
            generation_id=self.generation_id,
            nonce=NONCE,
        )


def _install(
    monkeypatch: pytest.MonkeyPatch,
    harness: _Harness,
    *,
    skip_health_wait: bool = True,
) -> _Harness:
    """Replace the real transport at the exact seams production code uses.

    ``skip_health_wait=False`` leaves ``frp_link.wait_for_channel_health``
    real -- used by exactly one test that needs the genuine timeout/subject
    text, never mocked away (#231 R5 opus review item R6b).
    """

    def process_factory(argv: list[str], **kwargs: object) -> _ManagedProcess:
        kind = "ssh" if argv and argv[0] == "ssh" else "frpc"
        process = _ManagedProcess(argv, kind=kind)
        harness.spawn_kwargs.append(dict(kwargs))
        if kind == "frpc":
            config_path = Path(argv[-1])
            config_text = config_path.read_text(encoding="utf-8")
            harness.visitor_configs.append(config_text)
            harness.visitor_config_paths.append(config_path)
            if harness.next_frpc_stdout is not None:
                process.stdout = io.BytesIO(harness.next_frpc_stdout)
                harness.next_frpc_stdout = None
            if harness.next_frpc_exit is not None:
                process.returncode = 1
                process.stderr = io.BytesIO(harness.next_frpc_exit)
                harness.next_frpc_exit = None
            elif harness.xtcp_should_fail and 'type = "xtcp"' in config_text:
                process.returncode = 1
                process.stderr = io.BytesIO(b"xtcp hole punching failed\n")
        harness.processes.append(process)
        return process

    def stream_factory(*_args: object, **kwargs: object) -> _Stream:
        timeout = kwargs.get("timeout", 30.0)
        stream = _Stream(harness, float(cast(float, timeout)))
        harness.streams.append(stream)
        return stream

    def skip_health(*_args: object, **_kwargs: object) -> None:
        """The tunnel is faked, so its loopback readiness probe is a no-op."""

    def fixed_nonce(_size: int) -> str:
        return NONCE

    monkeypatch.setattr("clio_relay.frp_link.spawn_frp_process", process_factory)
    monkeypatch.setattr("clio_relay.control_channel.spawn_channel_process", process_factory)
    if skip_health_wait:
        monkeypatch.setattr("clio_relay.frp_link.wait_for_channel_health", skip_health)
    monkeypatch.setattr("clio_relay.remote_connection.secrets.token_hex", fixed_nonce)
    monkeypatch.setattr("clio_relay.remote_connection.http.client.HTTPConnection", stream_factory)
    monkeypatch.setattr("clio_relay.frp_transport.http.client.HTTPConnection", stream_factory)
    monkeypatch.setattr("clio_relay.session_api.connection_registry", lambda: harness.registry)
    return harness


def _set_frp_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    token: str = "frp-token-from-env",
    secret: str = "stcp-secret-from-env",
) -> None:
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", token)
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", secret)


def _frp_definition(name: str = "ares") -> ClusterDefinition:
    """Return a cluster definition that has opted into the §8.3 identity anchor."""
    return ClusterDefinition(
        name=name,
        ssh_host=f"{name}-login",
        frp_transport=FrpTransportConfig(
            server_addr="relay.example.org",
            identity_anchor="preshared_link_secret",
        ),
    )


def _settings(
    tmp_path: Path,
    *,
    cluster: str = "ares",
    mode: TransportMode = "brokered_tcp",
    interactive: bool = False,
) -> RelaySettings:
    # `interactive=False` is the realistic config for these modes: there is no
    # ssh 2FA prompt to wait for, so a real deployment turns the connection-level
    # authorization-required announcement off (see
    # test_brokered_mode_requires_no_interactive_authorization below).
    return RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster=cluster,
        remote_transport_mode=mode,
        remote_transport_interactive=interactive,
    )


def _connect(
    tmp_path: Path,
    harness: _Harness,
    *,
    mode: TransportMode = "brokered_tcp",
    definition: ClusterDefinition | None = None,
    interactive: bool = False,
    timeout_seconds: float | None = None,
) -> Any:
    """Establish the connection for the default cluster and return it."""
    return harness.registry.connection(
        definition=definition or _frp_definition(),
        settings=_settings(tmp_path, mode=mode, interactive=interactive),
        timeout_seconds=timeout_seconds if timeout_seconds is not None else 30.0,
    )


def test_brokered_bring_up_performs_exactly_one_frp_pair_and_zero_ssh_dials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())

    connection = _connect(tmp_path, harness)

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0
    assert connection.connected is True
    assert [event.event for event in connection.events] == ["establishing", "established"]
    assert connection.events[-1].mode == "brokered_tcp"
    assert connection.events[-1].identity_anchor == "preshared_link_secret"
    # Bring-up fetched status+identity over the held link, not an ssh act.
    paths = [cast(str, request["path"]) for request in harness.requests]
    assert "/session-status" in paths
    assert any(path.startswith("/session-identity") for path in paths)
    # Identity-first, always (§8.3/R1): even on the successful path, the
    # unauthenticated identity challenge is fetched and verified strictly
    # before the bearer-authenticated status request.
    identity_index = next(i for i, path in enumerate(paths) if path.startswith("/session-identity"))
    status_index = paths.index("/session-status")
    assert identity_index < status_index


def test_identity_first_bring_up_refuses_before_any_authenticated_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R1 [security, HIGH]: a rogue loopback responder must learn nothing.

    Demonstrated by the review: pre-fix, the AUTHENTICATED /session-status
    (bearer token + owner headers) was request #0 over the still-unverified
    tunnel -- a rogue process squatting on the local loopback bind, holding no
    secret of its own, received the real owner bearer token and reached
    connected==True. Post-fix, the unauthenticated /session-identity
    challenge is fetched and verified against this connection's PINNED
    identity FIRST; a rogue that answers with the wrong identity (it cannot
    know the real one) never sees a single authenticated request, and no
    Authorization header is ever sent to it.
    """
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.rogue_identity = True

    with pytest.raises(ChannelBootstrapError, match="pinned"):
        _connect(tmp_path, harness)

    paths = [cast(str, request["path"]) for request in harness.requests]
    assert not any(path == "/session-status" for path in paths)
    assert any(path.startswith("/session-identity") for path in paths)
    assert all(
        "Authorization" not in cast(dict[str, str], request["headers"])
        for request in harness.requests
    )
    # The typed refusal, not a spawned second attempt.
    assert harness.frp_pairs == 1


def test_ten_mixed_owned_session_operations_add_no_frp_pairs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The acceptance criterion: any number of operations, still one tunnel."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    definition = _frp_definition()
    settings = _settings(tmp_path)

    for index in range(10):
        with OwnedSessionApiClient(definition=definition, settings=settings) as client:
            client.request_json(method="GET", path=f"/jobs/job_{index}/status")
            client.request_json(method="POST", path=f"/jobs/job_{index}/wait", body={})

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0
    operation_paths = [
        request["path"]
        for request in harness.requests
        if cast(str, request["path"]).startswith("/jobs/")
    ]
    assert len(operation_paths) == 20


def test_dropped_visitor_never_respawns_without_an_explicit_reconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unattended redial is the violation; the drop must surface instead."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)

    harness.processes[-1].drop()

    with pytest.raises(ChannelDropped, match="call reconnect"):
        connection.request_json(method="GET", path="/jobs/job_1/status")

    assert harness.frp_pairs == 1
    dropped = [event for event in connection.events if event.event == "dropped"]
    assert len(dropped) == 1
    assert dropped[0].reason == "transport_exited"
    assert dropped[0].mode == "brokered_tcp"

    with pytest.raises(ChannelDropped, match="call reconnect"):
        connection.connect()

    assert harness.frp_pairs == 1


def test_reconnect_performs_exactly_one_new_frp_pair_and_emits_typed_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    harness.processes[-1].drop()
    with pytest.raises(ChannelDropped):
        connection.request_json(method="GET", path="/jobs/job_1/status")

    connection.reconnect()

    assert harness.frp_pairs == 2
    assert harness.ssh_dials == 0
    assert connection.connected is True
    names = [event.event for event in connection.events]
    assert names == [
        "establishing",
        "established",
        "dropped",
        "reestablishing",
        "establishing",
        "reestablished",
    ]
    for event in connection.events:
        assert event.mode == "brokered_tcp"
        assert event.identity_anchor == "preshared_link_secret"

    connection.request_json(method="GET", path="/jobs/job_1/status")
    assert harness.frp_pairs == 2


def test_close_releases_the_visitor_and_removes_its_rendered_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    config_path = harness.visitor_config_paths[-1]
    assert config_path.exists()

    connection.close()

    assert connection.connected is False
    assert harness.processes[-1].terminated is True
    assert not config_path.exists()
    closed = connection.events[-1]
    assert closed.event == "closed"
    # Sabotage twin of test_close_surfaces_a_residual_config_cleanup_error_as_a_typed_event
    # below: an ordinary close never fabricates a reason.
    assert closed.reason is None
    assert closed.detail is None


def test_close_surfaces_a_residual_config_cleanup_error_as_a_typed_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#231 R5 opus review item R3: HeldFrpVisitor.config_cleanup_error must
    reach the ledger, not stay buried inside the substrate.

    SABOTAGE: monkeypatches TemporaryDirectory.cleanup to always raise
    (mirrors tests/test_frp_link.py's own F3 sabotage), proving the residual,
    secret-bearing config file is surfaced as a typed reason on the "closed"
    event rather than silently dropped.
    """
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)

    def raising_cleanup(self: tempfile.TemporaryDirectory[str]) -> None:
        del self
        raise OSError("simulated: the config file is still held open")

    monkeypatch.setattr(tempfile.TemporaryDirectory, "cleanup", raising_cleanup)

    connection.close()

    closed = connection.events[-1]
    assert closed.event == "closed"
    assert closed.reason == "config_cleanup_error"
    assert closed.detail is not None
    assert "token/secret" in closed.detail


def test_visitor_exit_message_prefixes_mode_visitor_type_and_cluster(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#231 R5 opus review item R4 (F2 shape): the prefix naming which link
    this is must never be REPLACED by the visitor's bounded stdout/stderr
    excerpt, only extended by it.
    """
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.next_frpc_exit = b"login to server failed: EOF\n"

    with pytest.raises(RelayError) as exc_info:
        _connect(tmp_path, harness)

    message = str(exc_info.value)
    assert message.startswith("brokered_tcp stcp visitor for cluster 'ares' exited immediately:")
    assert "login to server failed" in message


def test_udp_rendezvous_punch_failure_is_a_typed_refusal_never_falling_back_to_stcp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SABOTAGE TWIN: a punch failure must never render or spawn an stcp visitor."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.xtcp_should_fail = True
    render_calls: list[str] = []
    original_render = frp_link.render_visitor_config

    def spy_render(config: Any, **kwargs: Any) -> str:
        render_calls.append(cast(str, kwargs.get("visitor_type", "stcp")))
        return original_render(config, **kwargs)

    monkeypatch.setattr("clio_relay.frp_link.render_visitor_config", spy_render)

    with pytest.raises(TransportPunchFailed, match="hole punch failed"):
        _connect(tmp_path, harness, mode="udp_rendezvous")

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0
    assert len(harness.processes) == 1
    # Render-scoped, not just text-scoped (#231 R5 opus review item R6d):
    # proves render_visitor_config itself was never invoked for stcp, not
    # merely that the resulting rendered text happens not to contain the
    # substring.
    assert render_calls == ["xtcp"]
    assert harness.visitor_configs
    assert all('type = "xtcp"' in config for config in harness.visitor_configs)
    assert all('type = "stcp"' not in config for config in harness.visitor_configs)


def test_udp_rendezvous_bring_up_performs_exactly_one_frp_pair_with_xtcp_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#231 R5 opus review item R6a: the successful udp_rendezvous half."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())

    connection = _connect(tmp_path, harness, mode="udp_rendezvous")

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0
    assert connection.connected is True
    assert connection.events[-1].mode == "udp_rendezvous"
    assert connection.events[-1].identity_anchor == "preshared_link_secret"
    rendered = harness.visitor_configs[-1]
    assert 'type = "xtcp"' in rendered
    assert "keepTunnelOpen = true" in rendered


def test_udp_rendezvous_health_timeout_is_a_typed_punch_failure_naming_the_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#231 R5 opus review item R6b: the health-timeout half of TransportPunchFailed.

    Deliberately does NOT mock ``wait_for_channel_health``: a real, bounded
    wait against a loopback port nothing is listening on times out fast
    (connection-refused is near-instant) and exercises the genuine subject
    text ("frp xtcp link", #231 R4 opus review F4) end to end.
    """
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness(), skip_health_wait=False)

    with pytest.raises(TransportPunchFailed, match="frp xtcp link"):
        _connect(tmp_path, harness, mode="udp_rendezvous", timeout_seconds=0.3)

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0


def test_held_visitor_spawns_with_piped_stdout_and_the_pump_attaches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#231 R5 opus review item R6c: lock the spawn kwargs.

    A DEVNULL regression (frpc's chatty stdout by default, #231 R4 opus
    review F1) must go red HERE -- a fast unit-test assertion -- not silently
    wedge a real frpc child hours later in a live deployment.
    """
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.next_frpc_stdout = b"frpc: login to server success\n"
    harness.next_frpc_exit = b"connection lost after login\n"

    with pytest.raises(RelayError) as exc_info:
        _connect(tmp_path, harness)

    assert harness.spawn_kwargs
    frpc_kwargs = harness.spawn_kwargs[-1]
    assert frpc_kwargs.get("stdout") is subprocess.PIPE
    assert frpc_kwargs.get("stderr") is subprocess.PIPE
    # The pump attached and actually read it -- not devnull'd, not ignored.
    assert "login to server success" in str(exc_info.value)


def test_open_stream_channel_refuses_multiplexing() -> None:
    """Multiplexing onto the held link is not built for any mode yet."""
    transport = BrokeredTcpTransport(
        definition=_frp_definition(),
        cluster="ares",
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=DEFAULT_OWNED_SESSION_API_PORT,
        api_token="session-api-token",
        identity_anchor="preshared_link_secret",
        frpc_bin="frpc",
    )

    with pytest.raises(StreamChannelsUnavailable, match="not yet carry"):
        transport.open_stream_channel(name="watch", remote_port=9000)


def test_identity_anchor_prefers_the_held_links_snapshot_over_drifted_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#231 R5 opus review item R9: the audit trail describes the link, not
    whatever the on-disk cluster definition says right now.
    """
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    assert connection.identity_anchor == "preshared_link_secret"

    # Simulate the cluster definition drifting on disk after bring-up: the
    # HELD link's own snapshot must still win.
    connection._definition.frp_transport.identity_anchor = None  # pyright: ignore[reportPrivateUsage]

    assert connection.identity_anchor == "preshared_link_secret"


def test_established_permanently_consumes_a_failed_transport_instance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#231 R5 opus review item R10: matches SshForwardTransport's reference
    lifecycle -- a failed establish never permits a retry in place. The
    caller must build a NEW transport (exactly what
    RemoteConnection.reconnect() does via build_transport()).
    """
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.rogue_identity = True
    transport = BrokeredTcpTransport(
        definition=_frp_definition(),
        cluster="ares",
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=DEFAULT_OWNED_SESSION_API_PORT,
        api_token="session-api-token",
        identity_anchor="preshared_link_secret",
        frpc_bin="frpc",
    )

    with pytest.raises(ChannelBootstrapError):
        transport.establish(nonce=NONCE)

    with pytest.raises(RelayError, match="already established"):
        transport.establish(nonce=NONCE)

    # The doomed retry attempt never reached the point of spawning a second pair.
    assert harness.frp_pairs == 1


def test_brokered_mode_requires_no_interactive_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unlike mode (c) (`remote_connection.py:411-420`), no prompt is ever expected.

    `BrokeredTcpTransport.requires_user_authorization` is unconditionally
    False: both relays simply dial out to the already-deployed relay point,
    there is no ssh act to authorize. #231 R5 opus review item R7: the
    connection-level `authorization_required` event is now gated on
    `transport.requires_user_authorization` itself (not on
    `settings.remote_transport_interactive`), so this is a STRUCTURAL
    property of the mode, not a fixture/settings choice -- proven here with
    the interactive policy left at its actual DEFAULT (`interactive=True`,
    the same value a fresh deployment gets with nothing configured).
    """
    transport = BrokeredTcpTransport(
        definition=_frp_definition(),
        cluster="ares",
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=DEFAULT_OWNED_SESSION_API_PORT,
        api_token="session-api-token",
        identity_anchor="preshared_link_secret",
        frpc_bin="frpc",
    )
    assert transport.requires_user_authorization is False

    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness, interactive=True)

    assert "authorization_required" not in [event.event for event in connection.events]


def test_unconfigured_identity_anchor_refuses_before_spawning_anything(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    unconfigured = ClusterDefinition(name="ares", ssh_host="ares-login")
    connection = RemoteConnection(definition=unconfigured, settings=_settings(tmp_path))

    with pytest.raises(TransportIdentityAnchorRequired, match="preshared_link_secret"):
        connection.connect()

    assert harness.processes == []
    assert harness.ssh_dials == 0
    assert harness.frp_pairs == 0
    # #231 R5 opus review item R2: the refusal must reach the ledger as a
    # terminal establish_failed event, not propagate with a dangling
    # establishing and no terminal event at all.
    names = [event.event for event in connection.events]
    assert names == ["establish_failed"]
    terminal = connection.events[-1]
    assert terminal.reason == "TransportIdentityAnchorRequired"
    assert terminal.detail is not None
    assert "preshared_link_secret" in terminal.detail


def test_identity_anchor_appears_in_event_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    _connect(tmp_path, harness)

    report = harness.registry.event_report()

    cluster_report = cast(dict[str, object], cast(dict[str, object], report["clusters"])["ares"])
    assert cluster_report["identity_anchor"] == "preshared_link_secret"
    events = cast(list[dict[str, object]], cluster_report["events"])
    assert events
    assert all(event["identity_anchor"] == "preshared_link_secret" for event in events)


def test_visitor_secret_comes_from_the_cluster_env_binding_not_a_literal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch, token="frp-token-xyz", secret="stcp-secret-xyz")
    harness = _install(monkeypatch, _Harness())

    _connect(tmp_path, harness)

    assert len(harness.visitor_configs) == 1
    rendered = harness.visitor_configs[0]
    assert "stcp-secret-xyz" in rendered
    assert "frp-token-xyz" in rendered


def test_missing_frp_env_binding_refuses_rather_than_using_a_literal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    harness = _install(monkeypatch, _Harness())

    with pytest.raises(ConfigurationError, match="CLIO_RELAY_FRP_TOKEN"):
        _connect(tmp_path, harness)

    assert harness.processes == []

"""The one-held-tunnel invariant for modes (a)/(b) (#231 R5, #188 items 1-3+5).

Cloned from ``tests/test_owned_session_channel.py``'s harness: a fake process
factory records every dial, keyed on argv so an ``frpc`` spawn (one held frp
visitor "pair") and an ``ssh`` spawn (one dial) are counted separately. The
headline assertion throughout is ``ssh_dials == 0`` -- these modes must never
fall back to, or accidentally reach, an ssh dial. No sockets, no real
``frpc``, no cluster: only a temp dir for the rendered visitor TOML, which
``HeldFrpVisitor`` itself still writes for real.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.config import RelaySettings, TransportMode
from clio_relay.control_channel import ChannelDropped, TransportIdentityAnchorRequired
from clio_relay.errors import ConfigurationError
from clio_relay.frp_transport import BrokeredTcpTransport, TransportPunchFailed
from clio_relay.remote_connection import DEFAULT_OWNED_SESSION_API_PORT, RemoteConnectionRegistry
from clio_relay.session_api import OwnedSessionApiClient, session_identity_document

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
        self.visitor_configs: list[str] = []
        self.visitor_config_paths: list[Path] = []
        self.xtcp_should_fail = False
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


def _install(monkeypatch: pytest.MonkeyPatch, harness: _Harness) -> _Harness:
    """Replace the real transport at the exact seams production code uses."""

    def process_factory(argv: list[str], **_kwargs: object) -> _ManagedProcess:
        kind = "ssh" if argv and argv[0] == "ssh" else "frpc"
        process = _ManagedProcess(argv, kind=kind)
        if kind == "frpc":
            config_path = Path(argv[-1])
            config_text = config_path.read_text(encoding="utf-8")
            harness.visitor_configs.append(config_text)
            harness.visitor_config_paths.append(config_path)
            if harness.xtcp_should_fail and 'type = "xtcp"' in config_text:
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
) -> Any:
    """Establish the connection for the default cluster and return it."""
    return harness.registry.connection(
        definition=definition or _frp_definition(),
        settings=_settings(tmp_path, mode=mode),
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
    assert [event.event for event in connection.events][-1] == "closed"


def test_udp_rendezvous_punch_failure_is_a_typed_refusal_never_falling_back_to_stcp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SABOTAGE TWIN: a punch failure must never render or spawn an stcp visitor."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.xtcp_should_fail = True

    with pytest.raises(TransportPunchFailed, match="hole punch failed"):
        _connect(tmp_path, harness, mode="udp_rendezvous")

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0
    assert len(harness.processes) == 1
    assert harness.visitor_configs
    assert all('type = "xtcp"' in config for config in harness.visitor_configs)
    assert all('type = "stcp"' not in config for config in harness.visitor_configs)


def test_brokered_mode_requires_no_interactive_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unlike mode (c) (`remote_connection.py:411-420`), no prompt is ever expected.

    `BrokeredTcpTransport.requires_user_authorization` is unconditionally False:
    both relays simply dial out to the already-deployed relay point, there is no
    ssh act to authorize. The connection-level `authorization_required` event is
    a separate, `RemoteConnection`-owned announcement gated on the deployment's
    own interactive policy (`settings.remote_transport_interactive`) rather than
    on the transport itself; a real brokered_tcp deployment turns that off
    because it has no prompt to announce, which is exactly the config this
    harness uses (see `_settings`'s `interactive=False` default).
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
    connection = _connect(tmp_path, harness)

    assert "authorization_required" not in [event.event for event in connection.events]


def test_unconfigured_identity_anchor_refuses_before_spawning_anything(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    unconfigured = ClusterDefinition(name="ares", ssh_host="ares-login")

    with pytest.raises(TransportIdentityAnchorRequired, match="preshared_link_secret"):
        _connect(tmp_path, harness, definition=unconfigured)

    assert harness.processes == []
    assert harness.ssh_dials == 0
    assert harness.frp_pairs == 0


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

"""The one held frp link: one visitor per link, never an ssh dial.

These tests drive :class:`~clio_relay.frp_transport.BrokeredTcpTransport` and
:class:`~clio_relay.frp_transport.UdpRendezvousTransport` directly, and the
worker's :class:`~clio_relay.adapters.worker.link.WorkerConnection` on top of
them. A fake process factory records every spawn, keyed on argv, so an
``frpc`` spawn (one held visitor) and an ``ssh`` spawn (one dial) are counted
apart. A fake ``http.client.HTTPConnection`` plays the worker's site API. No
real ``frpc`` and no cluster: only a temp dir for the rendered visitor TOML,
which ``HeldFrpVisitor`` still writes for real. One health-timeout test leaves
``wait_for_channel_health`` real and lets it fail against a loopback port
nothing listens on.
"""

from __future__ import annotations

import io
import json
import subprocess
import urllib.parse
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay import frp_link
from clio_relay.adapters.worker.link import WORKER_PROTOCOL, WorkerConnection, WorkerOutdated
from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.control_channel import (
    ChannelBootstrapError,
    ChannelDropped,
    StreamChannelsUnavailable,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_transport import (
    BrokeredTcpTransport,
    TransportPunchFailed,
    UdpRendezvousTransport,
)
from clio_relay.frp_visitor_reconciliation import ReconciliationResult
from clio_relay.scratch import ScratchDir
from clio_relay.session_api import SESSION_IDENTITY_SCHEMA, session_identity_document

NONCE = "1" * 64
OWNER_TOKEN = "owner-token"
API_PORT = 8765
SESSION = "desktop-session-1"
GENERATION = "generation-1"


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
    """One fake HTTP connection over the held tunnel."""

    def __init__(self, harness: _Harness, timeout: float) -> None:
        self.harness = harness
        self.auto_open = 1
        self.socket = _Socket(timeout)
        self.sock: object | None = None
        self.paths: list[str] = []

    def connect(self) -> None:
        if self.harness.link_down:
            raise ConnectionRefusedError("the visitor is gone")
        self.sock = self.socket

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        if self.harness.link_down:
            raise ConnectionResetError("the visitor is gone")
        self.paths.append(path)
        self.harness.requests.append(
            {"method": method, "path": path, "body": body, "headers": dict(headers or {})}
        )

    def getresponse(self) -> _Response:
        path = self.paths[-1]
        if path.startswith("/session-identity"):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
            nonce = query["nonce"][0]
            if self.harness.rogue_identity:
                # A responder that does not know the pinned identity can only guess.
                return _Response(
                    {
                        "schema_version": SESSION_IDENTITY_SCHEMA,
                        "cluster": "not-the-pinned-cluster",
                        "session_id": "not-the-pinned-session",
                        "session_generation_id": "not-the-pinned-generation",
                        "nonce": nonce,
                        "hmac_sha256": "0" * 64,
                    }
                )
            return _Response(
                session_identity_document(
                    owner_token=self.harness.owner_token,
                    cluster=self.harness.cluster,
                    session_id=SESSION,
                    generation_id=GENERATION,
                    nonce=nonce,
                )
            )
        if path == "/session-status":
            return _Response(self.harness.status)
        return _Response({"ok": True, "path": path})

    def close(self) -> None:
        self.sock = None


class _ManagedProcess:
    """One fake spawned process: one of these is one dial or one held visitor."""

    def __init__(self, argv: list[str], *, kind: str) -> None:
        self.argv = argv
        self.kind = kind
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
        """Simulate the held visitor dying while the link holds it."""
        self.returncode = 255


class _Harness:
    """Records every spawn, every rendered config and every request."""

    def __init__(self, *, cluster: str = "ares") -> None:
        self.cluster = cluster
        self.owner_token = OWNER_TOKEN
        self.processes: list[_ManagedProcess] = []
        self.spawn_kwargs: list[dict[str, object]] = []
        self.visitor_configs: list[str] = []
        self.visitor_config_paths: list[Path] = []
        self.xtcp_should_fail = False
        # One-shot injections consumed by the next frpc spawn only.
        self.next_frpc_stdout: bytes | None = None
        self.next_frpc_exit: bytes | None = None
        self.rogue_identity = False
        self.requests: list[dict[str, object]] = []
        self.streams: list[_Stream] = []
        self.status: dict[str, object] = {
            "owner": "clio-relay",
            "cluster": cluster,
            "session_id": SESSION,
            "session_generation_id": GENERATION,
            "remote_api_port": API_PORT,
            "running": True,
            "ownership_verified": True,
            "worker_protocol": WORKER_PROTOCOL,
        }
        self.reconciliation_calls: list[str] = []
        self.reconciliation_result = ReconciliationResult(reaped_pids=(), swept_config_dirs=0)

    @property
    def ssh_dials(self) -> int:
        return sum(1 for process in self.processes if process.kind == "ssh")

    @property
    def frp_pairs(self) -> int:
        return sum(1 for process in self.processes if process.kind == "frpc")

    @property
    def link_down(self) -> bool:
        frpc = [process for process in self.processes if process.kind == "frpc"]
        return bool(frpc) and frpc[-1].returncode is not None

    def paths(self) -> list[str]:
        return [cast(str, request["path"]) for request in self.requests]


def _install(
    monkeypatch: pytest.MonkeyPatch,
    harness: _Harness,
    *,
    skip_health_wait: bool = True,
) -> _Harness:
    """Replace the real process, socket and reconciliation seams."""

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
        stream = _Stream(harness, float(cast(float, kwargs.get("timeout", 30.0))))
        harness.streams.append(stream)
        return stream

    def skip_health(*_args: object, **_kwargs: object) -> None:
        """The tunnel is faked, so its loopback readiness probe is a no-op."""

    def reconcile(**_kwargs: object) -> ReconciliationResult:
        harness.reconciliation_calls.append("visitor")
        return harness.reconciliation_result

    monkeypatch.setattr("clio_relay.frp_link.spawn_frp_process", process_factory)
    if skip_health_wait:
        monkeypatch.setattr("clio_relay.frp_link.wait_for_channel_health", skip_health)
    monkeypatch.setattr("clio_relay.frp_transport.http.client.HTTPConnection", stream_factory)
    monkeypatch.setattr("clio_relay.frp_transport.reconcile_stale_frp_visitors", reconcile)
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
    return ClusterDefinition(
        name=name,
        ssh_host=f"{name}-login",
        frp_transport=FrpTransportConfig(
            server_addr="relay.example.org",
            identity_anchor="preshared_link_secret",
        ),
    )


def _transport(
    *,
    udp: bool = False,
    ready_timeout_seconds: float = 30.0,
) -> BrokeredTcpTransport | UdpRendezvousTransport:
    kind = UdpRendezvousTransport if udp else BrokeredTcpTransport
    return kind(
        definition=_frp_definition(),
        cluster="ares",
        session_id=SESSION,
        session_generation_id=GENERATION,
        remote_api_port=API_PORT,
        api_token="session-api-token",
        identity_anchor="preshared_link_secret",
        ready_timeout_seconds=ready_timeout_seconds,
    )


def _worker() -> WorkerConnection:
    return WorkerConnection(
        definition=_frp_definition(),
        session_id=SESSION,
        generation_id=GENERATION,
        remote_api_port=API_PORT,
        api_token="session-api-token",
        session_owner_token=OWNER_TOKEN,
    )


def test_brokered_bring_up_performs_exactly_one_frp_pair_and_zero_ssh_dials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    transport = _transport()

    link = transport.establish(nonce=NONCE)

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0
    assert transport.is_alive() is True
    assert transport.mode == "brokered_tcp"
    assert link.identity_anchor == "preshared_link_secret"
    # Identity first: the unauthenticated challenge is verified before the
    # bearer-authenticated status request goes out.
    paths = harness.paths()
    identity_index = next(i for i, path in enumerate(paths) if path.startswith("/session-identity"))
    assert identity_index < paths.index("/session-status")


def test_identity_first_bring_up_refuses_before_any_authenticated_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rogue loopback responder must never see the bearer token."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.rogue_identity = True

    with pytest.raises(ChannelBootstrapError, match="pinned"):
        _transport().establish(nonce=NONCE)

    assert "/session-status" not in harness.paths()
    assert any(path.startswith("/session-identity") for path in harness.paths())
    assert all(
        "Authorization" not in cast(dict[str, str], request["headers"])
        for request in harness.requests
    )
    assert harness.frp_pairs == 1
    assert harness.processes[-1].terminated is True


def test_twenty_worker_calls_ride_one_frp_pair_and_each_stream_proves_identity_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    worker = _worker()
    worker.connect()

    for index in range(10):
        worker.request_json(method="GET", path=f"/jobs/job_{index}/status")
        worker.request_json(method="POST", path=f"/jobs/job_{index}/wait", body={})

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0
    assert len([path for path in harness.paths() if path.startswith("/jobs/")]) == 20
    for stream in harness.streams:
        if any(path.startswith("/jobs/") for path in stream.paths):
            assert stream.paths[0].startswith("/session-identity")
    worker.close()


def test_a_dropped_visitor_surfaces_and_is_never_respawned_by_the_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    worker = _worker()
    worker.connect()
    worker.request_json(method="GET", path="/jobs/job_1/status")

    harness.processes[-1].drop()

    assert worker.connected is False
    with pytest.raises(ChannelDropped):
        worker.request_json(method="GET", path="/jobs/job_1/status")
    with pytest.raises(ChannelDropped):
        worker.request_json(method="GET", path="/jobs/job_1/status")
    assert harness.frp_pairs == 1


def test_a_new_link_after_a_drop_holds_exactly_one_new_frp_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    first = _worker()
    first.connect()
    harness.processes[-1].drop()
    first.close()

    second = _worker()
    second.connect()
    second.request_json(method="GET", path="/jobs/job_1/status")

    assert harness.frp_pairs == 2
    assert harness.ssh_dials == 0
    assert second.connected is True
    second.close()


def test_an_older_worker_is_refused_as_outdated_and_its_visitor_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    del harness.status["worker_protocol"]
    worker = _worker()

    with pytest.raises(WorkerOutdated, match="older relay"):
        worker.connect()

    assert worker.connected is False
    assert harness.processes[-1].terminated is True


def test_a_worker_that_cannot_sign_with_the_owner_token_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.owner_token = "some-other-owner-token"
    worker = _worker()

    with pytest.raises(RelayError, match="did not prove its identity"):
        worker.connect()

    assert harness.processes[-1].terminated is True


def test_close_releases_the_visitor_and_removes_its_rendered_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    transport = _transport()
    transport.establish(nonce=NONCE)
    config_path = harness.visitor_config_paths[-1]
    assert config_path.exists()

    transport.close()

    assert transport.is_alive() is False
    assert harness.processes[-1].terminated is True
    assert not config_path.exists()
    assert transport.failure_detail() is None


def test_close_keeps_a_residual_config_cleanup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rendered config holds secrets, so a failed cleanup is never dropped."""
    _set_frp_env(monkeypatch)
    _install(monkeypatch, _Harness())
    transport = _transport()
    transport.establish(nonce=NONCE)

    def raising_cleanup(self: ScratchDir) -> None:
        del self
        raise OSError("simulated: the config file is still held open")

    monkeypatch.setattr(ScratchDir, "cleanup", raising_cleanup)

    transport.close()

    detail = transport.failure_detail()
    assert detail is not None
    assert "token/secret" in detail


def test_reconciliation_runs_for_the_configured_frpc_and_reports_reaped_pids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.reconciliation_result = ReconciliationResult(
        reaped_pids=(4321, 4322), swept_config_dirs=1
    )
    transport = _transport()

    transport.establish(nonce=NONCE)

    assert transport.reaped_orphan_visitor_pids == (4321, 4322)
    assert transport.swept_stale_config_dirs == 1
    assert transport.reconciliation_skipped_reason is None
    assert harness.reconciliation_calls == ["visitor"]


def test_no_orphans_means_nothing_reaped(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    transport = _transport()

    transport.establish(nonce=NONCE)

    assert transport.reaped_orphan_visitor_pids == ()
    assert harness.reconciliation_calls == ["visitor"]


def test_a_skipped_snapshot_is_reported_rather_than_read_as_no_orphans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.reconciliation_result = ReconciliationResult(
        reaped_pids=(), swept_config_dirs=0, skipped_reason="snapshot_unavailable:timeout"
    )
    transport = _transport()

    transport.establish(nonce=NONCE)

    assert transport.reconciliation_skipped_reason == "snapshot_unavailable:timeout"


def test_visitor_exit_message_prefixes_mode_visitor_type_and_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The prefix naming the link is extended by the visitor's output, never replaced."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.next_frpc_exit = b"login to server failed: EOF\n"

    with pytest.raises(RelayError) as exc_info:
        _transport().establish(nonce=NONCE)

    message = str(exc_info.value)
    assert message.startswith("brokered_tcp stcp visitor for cluster 'ares' exited immediately:")
    assert "login to server failed" in message


def test_udp_rendezvous_punch_failure_is_a_typed_refusal_never_falling_back_to_stcp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A punch failure never renders or spawns an stcp visitor."""
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
        _transport(udp=True).establish(nonce=NONCE)

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0
    assert render_calls == ["xtcp"]
    assert all('type = "xtcp"' in config for config in harness.visitor_configs)
    assert all('type = "stcp"' not in config for config in harness.visitor_configs)


def test_udp_rendezvous_bring_up_performs_exactly_one_frp_pair_with_xtcp_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    transport = _transport(udp=True)

    link = transport.establish(nonce=NONCE)

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0
    assert transport.mode == "udp_rendezvous"
    assert link.identity_anchor == "preshared_link_secret"
    rendered = harness.visitor_configs[-1]
    assert 'type = "xtcp"' in rendered
    assert "keepTunnelOpen = true" in rendered


def test_udp_rendezvous_health_timeout_is_a_typed_punch_failure_naming_the_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leaves ``wait_for_channel_health`` real against a port nothing listens on."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness(), skip_health_wait=False)

    with pytest.raises(TransportPunchFailed, match="frp xtcp link"):
        _transport(udp=True, ready_timeout_seconds=0.3).establish(nonce=NONCE)

    assert harness.frp_pairs == 1
    assert harness.ssh_dials == 0


def test_held_visitor_spawns_with_piped_stdout_and_the_pump_attaches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DEVNULL regression would wedge a chatty frpc; it has to fail here."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.next_frpc_stdout = b"frpc: login to server success\n"
    harness.next_frpc_exit = b"connection lost after login\n"

    with pytest.raises(RelayError) as exc_info:
        _transport().establish(nonce=NONCE)

    frpc_kwargs = harness.spawn_kwargs[-1]
    assert frpc_kwargs.get("stdout") is subprocess.PIPE
    assert frpc_kwargs.get("stderr") is subprocess.PIPE
    assert "login to server success" in str(exc_info.value)


def test_open_stream_channel_refuses_multiplexing() -> None:
    with pytest.raises(StreamChannelsUnavailable, match="cannot yet carry"):
        _transport().open_stream_channel(name="watch", remote_port=9000)


def test_established_permanently_consumes_a_failed_transport_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed establish is never retried in place; the caller builds a new transport."""
    _set_frp_env(monkeypatch)
    harness = _install(monkeypatch, _Harness())
    harness.rogue_identity = True
    transport = _transport()

    with pytest.raises(ChannelBootstrapError):
        transport.establish(nonce=NONCE)
    with pytest.raises(RelayError, match="already established"):
        transport.establish(nonce=NONCE)

    assert harness.frp_pairs == 1


def test_brokered_mode_requires_no_interactive_authorization() -> None:
    assert _transport().requires_user_authorization is False
    assert _transport(udp=True).requires_user_authorization is False


def test_visitor_secret_comes_from_the_cluster_env_binding_not_a_literal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_frp_env(monkeypatch, token="frp-token-xyz", secret="stcp-secret-xyz")
    harness = _install(monkeypatch, _Harness())

    _transport().establish(nonce=NONCE)

    assert len(harness.visitor_configs) == 1
    rendered = harness.visitor_configs[0]
    assert "stcp-secret-xyz" in rendered
    assert "frp-token-xyz" in rendered


def test_missing_frp_env_binding_refuses_rather_than_using_a_literal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    harness = _install(monkeypatch, _Harness())

    with pytest.raises(ConfigurationError, match="CLIO_RELAY_FRP_TOKEN"):
        _transport().establish(nonce=NONCE)

    assert harness.processes == []

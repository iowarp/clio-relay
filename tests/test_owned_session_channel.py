"""The one-held-channel invariant for the owned-session control plane.

These tests assert the design property directly, in the unit the deployment
gate measures: how many new SSH connections the desktop opens.  One connection
is one call to the injected channel-process factory, so the counts here are the
same counts the two-sided acceptance harness reads from a process sampler and
the cluster's ``sshd`` session log.
"""

from __future__ import annotations

import hashlib
import io
import json
import threading
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.control_channel import (
    ChannelDropped,
    SshForwardTransport,
    TransportModeUnavailable,
    build_transport,
    owned_session_channel_bootstrap_script,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.http_api import create_app
from clio_relay.job_identity import OWNER_SESSION_ID_HEADER, SESSION_GENERATION_ID_HEADER
from clio_relay.remote_connection import (
    DEFAULT_OWNED_SESSION_API_PORT,
    RemoteConnectionRegistry,
    resolve_remote_api_port,
)
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
    """One fake proven HTTP stream over the held channel."""

    def __init__(self, harness: _Harness, timeout: float) -> None:
        self.harness = harness
        self.cluster = harness.dialing_cluster
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
            return _Response(self.harness.identity_for(self.cluster))
        return _Response(self.harness.responses.get(path, {"ok": True}))

    def close(self) -> None:
        self.closed = True
        self.sock = None


class _ChannelProcess:
    """One fake held-channel process: exactly one of these is one SSH dial."""

    def __init__(self, bootstrap: dict[str, object], argv: list[str]) -> None:
        self.argv = argv
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO(json.dumps(bootstrap).encode("utf-8") + b"\n")
        self.stderr = io.BytesIO(b"")
        self.returncode: int | None = None

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def drop(self) -> None:
        """Simulate the remote end dying while the local relay still holds it."""
        self.returncode = 255


class _Harness:
    """Records every dial and every request made over the held channel."""

    def __init__(self, *, generation_id: str = "generation-1") -> None:
        self.generation_id = generation_id
        self.processes: list[_ChannelProcess] = []
        self.streams: list[_Stream] = []
        self.requests: list[dict[str, object]] = []
        self.responses: dict[str, object] = {}
        self.registry = RemoteConnectionRegistry()
        self.dialing_cluster = "ares"

    @property
    def dials(self) -> int:
        """Return the number of new SSH connections opened so far."""
        return len(self.processes)

    @property
    def identity(self) -> dict[str, str]:
        """Return the identity document the default remote relay signs."""
        return self.identity_for("ares")

    def bootstrap(
        self,
        *,
        cluster: str = "ares",
        remote_api_port: int = DEFAULT_OWNED_SESSION_API_PORT,
    ) -> dict[str, object]:
        """Return the exact out-of-band document one bring-up dial reports."""
        return {
            "schema_version": "clio-relay.channel-bootstrap.v1",
            "status": {
                "owner": "clio-relay",
                "cluster": cluster,
                "session_id": "desktop-session-1",
                "session_generation_id": self.generation_id,
                "remote_api_port": remote_api_port,
                "running": True,
                "ownership_verified": True,
            },
            "identity": self.identity_for(cluster),
        }

    def identity_for(self, cluster: str) -> dict[str, str]:
        """Return the identity document one named remote relay signs."""
        return session_identity_document(
            owner_token=OWNER_TOKEN,
            cluster=cluster,
            session_id="desktop-session-1",
            generation_id=self.generation_id,
            nonce=NONCE,
        )


def _install(monkeypatch: pytest.MonkeyPatch, harness: _Harness) -> _Harness:
    """Replace the real transport at the exact seams production code uses."""

    def process_factory(argv: list[str], **_kwargs: object) -> _ChannelProcess:
        cluster = next(
            (item.removesuffix("-login") for item in argv if item.endswith("-login")),
            "ares",
        )
        harness.dialing_cluster = cluster
        process = _ChannelProcess(harness.bootstrap(cluster=cluster), argv)
        harness.processes.append(process)
        return process

    def stream_factory(*_args: object, **kwargs: object) -> _Stream:
        timeout = kwargs.get("timeout", 30.0)
        stream = _Stream(harness, float(cast(float, timeout)))
        harness.streams.append(stream)
        return stream

    def skip_health(*_args: object, **_kwargs: object) -> None:
        """The forward is faked, so its loopback readiness probe is a no-op."""

    def fixed_nonce(_size: int) -> str:
        return NONCE

    monkeypatch.setattr("clio_relay.control_channel.spawn_channel_process", process_factory)
    monkeypatch.setattr("clio_relay.control_channel._wait_for_channel_health", skip_health)
    monkeypatch.setattr("clio_relay.remote_connection.secrets.token_hex", fixed_nonce)
    monkeypatch.setattr("clio_relay.remote_connection.http.client.HTTPConnection", stream_factory)
    monkeypatch.setattr("clio_relay.session_api.connection_registry", lambda: harness.registry)
    return harness


def _settings(tmp_path: Path, *, cluster: str = "ares") -> RelaySettings:
    return RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster=cluster,
    )


def _definition(name: str = "ares") -> ClusterDefinition:
    return ClusterDefinition(name=name, ssh_host=f"{name}-login")


def _connect(tmp_path: Path, harness: _Harness) -> Any:
    """Establish the connection for the default cluster and return it."""
    return harness.registry.connection(
        definition=_definition(),
        settings=_settings(tmp_path),
    )


def test_channel_bring_up_performs_exactly_one_ssh_dial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install(monkeypatch, _Harness())

    connection = _connect(tmp_path, harness)

    assert harness.dials == 1
    assert connection.connected is True
    assert [event.event for event in connection.events] == [
        "authorization_required",
        "establishing",
        "established",
    ]
    assert connection.events[0].user_authorization_required is True
    assert connection.events[-1].mode == "ssh_forward"


def test_arbitrary_owned_session_operations_perform_zero_new_ssh_dials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The acceptance criterion: any number of operations, still one connection."""
    harness = _install(monkeypatch, _Harness())
    definition = _definition()
    settings = _settings(tmp_path)

    for index in range(12):
        with OwnedSessionApiClient(definition=definition, settings=settings) as client:
            client.request_json(method="GET", path=f"/jobs/job_{index}/status")
            client.request_json(method="POST", path=f"/jobs/job_{index}/wait", body={})
            client.request_json(method="GET", path=f"/artifacts/artifact_{index}/content")
            client.request_json(method="POST", path="/input-artifacts/ingest", body={})

    assert harness.dials == 1
    assert len(harness.streams) == 1
    operation_paths = [
        request["path"]
        for request in harness.requests
        if not cast(str, request["path"]).startswith("/session-identity")
    ]
    assert len(operation_paths) == 48
    assert "/artifacts/artifact_0/content" in operation_paths
    assert "/input-artifacts/ingest" in operation_paths


def test_dropped_channel_never_redials_without_an_explicit_reconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unattended redial is the violation; the drop must surface instead."""
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)

    harness.processes[0].drop()

    with pytest.raises(ChannelDropped, match="call reconnect"):
        connection.request_json(method="GET", path="/jobs/job_1/status")

    assert harness.dials == 1
    dropped = [event for event in connection.events if event.event == "dropped"]
    assert len(dropped) == 1
    assert dropped[0].reason == "transport_exited"

    with pytest.raises(ChannelDropped, match="call reconnect"):
        connection.connect()

    assert harness.dials == 1


def test_reconnect_performs_exactly_one_new_dial_and_emits_typed_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    harness.processes[0].drop()
    with pytest.raises(ChannelDropped):
        connection.request_json(method="GET", path="/jobs/job_1/status")

    connection.reconnect()

    assert harness.dials == 2
    assert connection.connected is True
    names = [event.event for event in connection.events]
    assert names == [
        "authorization_required",
        "establishing",
        "established",
        "dropped",
        "reestablishing",
        "authorization_required",
        "establishing",
        "reestablished",
    ]
    reestablishing = connection.events[names.index("reestablishing")]
    assert reestablishing.reason == "channel_dropped"
    assert reestablishing.user_authorization_required is True

    connection.request_json(method="GET", path="/jobs/job_1/status")
    assert harness.dials == 2


def test_reestablished_channel_serves_operations_without_further_dials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    harness.processes[0].drop()
    with pytest.raises(ChannelDropped):
        connection.request_json(method="GET", path="/jobs/job_1/status")
    connection.reconnect()

    for index in range(8):
        connection.request_json(method="GET", path=f"/jobs/job_{index}/status")

    assert harness.dials == 2


def test_broken_http_stream_is_reproven_over_the_same_channel_without_a_new_dial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dead TCP stream is not a dead channel; re-proving it costs no transport."""
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)

    harness.streams[0].close()
    connection.request_json(method="GET", path="/jobs/job_1/status")

    assert harness.dials == 1
    assert len(harness.streams) == 2
    assert [event.event for event in connection.events][-1] == "stream_reproven"
    identity_requests = [
        request
        for request in harness.requests
        if cast(str, request["path"]).startswith("/session-identity")
    ]
    assert len(identity_requests) == 2
    assert all("Authorization" not in cast(dict[str, str], r["headers"]) for r in identity_requests)


def test_one_local_relay_holds_one_channel_per_remote_relay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One local relay manages many remote relays; each holds exactly one channel."""
    harness = _install(monkeypatch, _Harness())

    ares = harness.registry.connection(
        definition=_definition("ares"),
        settings=_settings(tmp_path, cluster="ares"),
    )
    theta = harness.registry.connection(
        definition=_definition("theta"),
        settings=_settings(tmp_path, cluster="theta"),
    )

    assert harness.dials == 2
    assert harness.registry.clusters == ("ares", "theta")

    for _ in range(5):
        ares.request_json(method="GET", path="/jobs/job_1/status")
        theta.request_json(method="GET", path="/jobs/job_1/status")
    assert harness.dials == 2

    # Asking again for a cluster already connected reuses its held channel.
    assert (
        harness.registry.connection(
            definition=_definition("ares"),
            settings=_settings(tmp_path, cluster="ares"),
        )
        is ares
    )
    assert harness.dials == 2

    # One remote relay disconnecting leaves the other's channel untouched.
    harness.registry.disconnect("ares")
    assert harness.registry.clusters == ("theta",)
    assert harness.processes[0].poll() is not None
    assert theta.connected is True
    theta.request_json(method="GET", path="/jobs/job_1/status")
    assert harness.dials == 2


def test_channel_close_releases_the_held_process_and_records_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)

    connection.close()

    assert connection.connected is False
    assert harness.processes[0].poll() is not None
    assert [event.event for event in connection.events][-1] == "closed"
    with pytest.raises(RelayError):
        connection.request_json(method="GET", path="/jobs/job_1/status")
    assert harness.dials == 1


def test_bring_up_command_reports_status_and_identity_then_holds_the_forward() -> None:
    """One command carries the whole bring-up: no second dial for status or identity."""
    script = owned_session_channel_bootstrap_script(
        definition=_definition(),
        cluster="ares",
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        nonce=NONCE,
    )

    assert "session recovery-status --cluster ares --session-id desktop-session-1" in script
    assert "session challenge-owned" in script
    assert f'"nonce":"{NONCE}"' in script
    assert '"session_generation_id":"generation-1"' in script
    assert "clio-relay.channel-bootstrap.v1" in script
    # The trailing hold is what keeps the one SSH session (and its forward) up.
    assert script.rstrip().endswith("exec cat >/dev/null")


def test_ssh_forward_argv_maps_the_owned_api_port_and_allows_one_authorization() -> None:
    transport = SshForwardTransport(
        definition=_definition(),
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=8765,
        bootstrap_script="true",
    )

    argv = transport.argv(local_port=18_795)

    assert argv[:2] == ["ssh", "-T"]
    assert "-L" in argv
    assert argv[argv.index("-L") + 1] == "127.0.0.1:18795:127.0.0.1:8765"
    assert argv[-3:-1] == ["bash", "-lc"]
    assert "ares-login" in argv
    # The user is present for exactly this one connection, so it must be able
    # to prompt: BatchMode would make interactive two-factor approval fail.
    assert "BatchMode=yes" not in argv
    assert "ExitOnForwardFailure=yes" in argv
    assert transport.requires_user_authorization is True
    assert transport.mode == "ssh_forward"


def test_noninteractive_transport_opts_out_of_the_authorization_prompt() -> None:
    transport = SshForwardTransport(
        definition=_definition(),
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=8765,
        bootstrap_script="true",
        allow_interactive_authorization=False,
    )

    assert "BatchMode=yes" in transport.argv(local_port=18_795)
    assert transport.requires_user_authorization is False


@pytest.mark.parametrize("mode", ["brokered_tcp", "udp_rendezvous"])
def test_declared_transport_modes_refuse_instead_of_falling_back(mode: str) -> None:
    """An unbuilt mode must never quietly become per-operation SSH."""
    with pytest.raises(TransportModeUnavailable, match=mode):
        build_transport(
            mode=cast(Any, mode),
            definition=_definition(),
            session_id="desktop-session-1",
            session_generation_id="generation-1",
            remote_api_port=8765,
            nonce=NONCE,
        )


def test_bring_up_refuses_a_channel_that_maps_a_different_owned_api_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness()
    _install(monkeypatch, harness)
    def mismatched_port_factory(argv: list[str], **_kwargs: object) -> _ChannelProcess:
        process = _ChannelProcess(harness.bootstrap(remote_api_port=9999), argv)
        harness.processes.append(process)
        return process

    monkeypatch.setattr(
        "clio_relay.control_channel.spawn_channel_process",
        mismatched_port_factory,
    )

    with pytest.raises(RelayError, match="CLIO_RELAY_OWNER_SESSION_API_PORT"):
        _connect(tmp_path, harness)

    assert harness.dials == 1
    assert harness.streams == []


def test_remote_api_port_resolves_from_configuration_not_per_operation_discovery(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    assert resolve_remote_api_port(settings=settings) == DEFAULT_OWNED_SESSION_API_PORT
    assert resolve_remote_api_port(settings=settings, remote_api_port=18_795) == 18_795

    configured = settings.model_copy(update={"owner_session_api_port": 9001})
    assert resolve_remote_api_port(settings=configured) == 9001


def _owned_session_app_settings(tmp_path: Path) -> RelaySettings:
    return RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        owner_session_api_port=8765,
        session_owner_token="o" * 32,
    )


def _bind_authority(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    definition = ClusterDefinition(name="test-cluster", ssh_host="test-cluster")
    registry_path = tmp_path / "session-authority" / "clusters.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    payload = registry_path.read_bytes()
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_SESSION_REGISTRY_SHA256", hashlib.sha256(payload).hexdigest())
    monkeypatch.setenv("CLIO_RELAY_SESSION_ROUTE_REVISION", cluster_route_revision(definition))


def test_session_status_endpoint_reports_the_exact_live_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Status moved from an ``ssh bash -s`` script to the owned API."""
    _bind_authority(monkeypatch, tmp_path)
    client = cast(Any, TestClient(create_app(_owned_session_app_settings(tmp_path))))

    response = client.get(
        "/session-status",
        headers={
            "Authorization": "Bearer session-api-token",
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "schema_version": "clio-relay.owned-session-status.v1",
        "owner": "clio-relay",
        "cluster": "test-cluster",
        "session_id": "desktop-session-1",
        "session_generation_id": "generation-1",
        "remote_api_port": 8765,
        "running": True,
        "evidence": "live_api_self_report",
    }


def test_session_status_endpoint_requires_the_exact_owner_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_authority(monkeypatch, tmp_path)
    client = cast(Any, TestClient(create_app(_owned_session_app_settings(tmp_path))))

    assert client.get("/session-status").status_code == 401
    replaced = client.get(
        "/session-status",
        headers={
            "Authorization": "Bearer session-api-token",
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-2",
        },
    )
    assert replaced.status_code == 409


def test_session_status_over_the_held_channel_cross_checks_the_pinned_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    harness.responses["/session-status"] = {
        "schema_version": "clio-relay.owned-session-status.v1",
        "owner": "clio-relay",
        "cluster": "ares",
        "session_id": "desktop-session-1",
        "session_generation_id": "generation-1",
        "remote_api_port": DEFAULT_OWNED_SESSION_API_PORT,
        "running": True,
        "evidence": "live_api_self_report",
    }

    status = connection.session_status()

    assert status["session_generation_id"] == "generation-1"
    assert harness.dials == 1

    harness.responses["/session-status"] = {
        "schema_version": "clio-relay.owned-session-status.v1",
        "owner": "clio-relay",
        "cluster": "ares",
        "session_id": "desktop-session-1",
        "session_generation_id": "generation-9",
        "running": True,
        "evidence": "live_api_self_report",
    }
    with pytest.raises(RelayError, match="exact connected generation"):
        connection.session_status()
    assert harness.dials == 1


def test_identity_mismatch_over_the_channel_refuses_before_any_credential(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The HTTP listener must be the session the out-of-band bring-up proved."""
    harness = _install(monkeypatch, _Harness())

    forged = dict(harness.identity)
    forged["hmac_sha256"] = "f" * 64
    monkeypatch.setattr(
        "clio_relay.remote_connection._IDENTITY_FIELDS",
        ("schema_version", "cluster", "session_id", "session_generation_id", "nonce"),
    )
    original = _Stream.getresponse

    def forged_response(self: _Stream) -> _Response:
        path = cast(str, self.harness.requests[-1]["path"])
        if path.startswith("/session-identity"):
            return _Response(forged)
        return original(self)

    monkeypatch.setattr(_Stream, "getresponse", forged_response)

    with pytest.raises(RelayError, match="HMAC did not verify"):
        _connect(tmp_path, harness)

    assert harness.dials == 1
    authenticated = [
        request
        for request in harness.requests
        if "Authorization" in cast(dict[str, str], request["headers"])
    ]
    assert authenticated == []


def test_event_report_counts_the_transport_connections_the_gate_measures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The client half of the two-sided acceptance measurement."""
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    for index in range(10):
        connection.request_json(method="GET", path=f"/jobs/job_{index}/status")

    report = harness.registry.event_report()

    assert report["schema_version"] == "clio-relay.control-channel-report.v1"
    assert report["transport_connections_opened"] == 1
    ares = cast(dict[str, object], cast(dict[str, object], report["clusters"])["ares"])
    assert ares["transport_connections_opened"] == 1
    assert ares["transport_mode"] == "ssh_forward"
    assert ares["connected"] is True
    assert ares["remote_api_port"] == DEFAULT_OWNED_SESSION_API_PORT

    harness.processes[0].drop()
    with pytest.raises(ChannelDropped):
        connection.request_json(method="GET", path="/jobs/job_1/status")
    connection.reconnect()

    reconnected = harness.registry.event_report()
    assert reconnected["transport_connections_opened"] == 2
    assert harness.dials == 2


def test_concurrent_operations_share_the_channel_without_serializing_or_redialing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long poll on one operation must not block the rest of the cluster.

    Extra streams run through the forward that is already held, so concurrency
    costs more TCP connections but never another SSH connection.
    """
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    blocked = threading.Event()
    released = threading.Event()
    original_getresponse = _Stream.getresponse

    def slow_wait(self: _Stream) -> _Response:
        path = cast(str, self.harness.requests[-1]["path"])
        if path.endswith("/wait"):
            blocked.set()
            assert released.wait(timeout=10)
        return original_getresponse(self)

    monkeypatch.setattr(_Stream, "getresponse", slow_wait)

    waiter = threading.Thread(
        target=lambda: connection.request_json(method="POST", path="/jobs/job_1/wait", body={}),
        daemon=True,
    )
    waiter.start()
    assert blocked.wait(timeout=10)

    # The long poll is still in flight; other operations must proceed anyway.
    for index in range(4):
        connection.request_json(method="GET", path=f"/jobs/job_{index}/status")

    released.set()
    waiter.join(timeout=10)
    assert waiter.is_alive() is False

    assert harness.dials == 1
    assert len(harness.streams) == 2


def test_registry_reconnect_is_the_single_explicit_reestablish_entry_point(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    harness.processes[0].drop()

    reconnected = harness.registry.reconnect("ares")

    assert reconnected is connection
    assert harness.dials == 2
    assert connection.connected is True

    with pytest.raises(ConfigurationError, match="no owned session connection is held"):
        harness.registry.reconnect("theta")
    assert harness.dials == 2

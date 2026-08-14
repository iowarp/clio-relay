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
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from clio_relay import remote_connection
from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.control_channel import (
    CHANNEL_BOOTSTRAP_BEGIN,
    CHANNEL_BOOTSTRAP_END,
    ChannelBootstrapError,
    ChannelDropped,
    SshForwardTransport,
    StreamChannelsUnavailable,
    TransportIdentityAnchorRequired,
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

    def __init__(
        self,
        bootstrap: dict[str, object],
        argv: list[str],
        *,
        banner: bytes = b"",
        indent: int | None = None,
    ) -> None:
        self.argv = argv
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO(
            banner
            + CHANNEL_BOOTSTRAP_BEGIN
            + b"\n"
            + json.dumps(bootstrap, indent=indent).encode("utf-8")
            + b"\n"
            + CHANNEL_BOOTSTRAP_END
            + b"\n"
        )
        self.stderr = io.BytesIO(b"")
        self.returncode: int | None = None
        self.ignores_stdin_close = False
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        if not self.ignores_stdin_close:
            self.returncode = -15

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        if self.ignores_stdin_close:
            # A real ssh that does not notice the closed pipe: close() must
            # escalate to terminate() and, failing that, kill().
            raise subprocess.TimeoutExpired(cmd="ssh", timeout=5)
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


def test_stale_pooled_stream_is_identity_bound_reconnected_and_the_retry_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """clio-relay#213: a stream that dies between requests gets one retry.

    Unlike ``test_broken_http_stream_is_reproven_over_the_same_channel_without_a_new_dial``
    (which simulates a stream Python already knows is dead before it is used),
    this simulates the real defect: the stream looks live when it leaves the
    pool and only fails on the actual I/O, the way an OS-level idle close
    (WinError 10053/10054) really surfaces.
    """
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    original_getresponse = _Stream.getresponse
    failures_remaining = 1

    def flaky_getresponse(self: _Stream) -> _Response:
        nonlocal failures_remaining
        path = cast(str, self.harness.requests[-1]["path"])
        if path == "/jobs/job_1/status" and failures_remaining > 0:
            failures_remaining -= 1
            raise ConnectionResetError(
                "[WinError 10054] An existing connection was forcibly closed by the remote host"
            )
        return original_getresponse(self)

    monkeypatch.setattr(_Stream, "getresponse", flaky_getresponse)

    result = connection.request_json(method="GET", path="/jobs/job_1/status")

    assert result == {"ok": True}
    # A stream retry, never a channel reconnect: no new SSH dial.
    assert harness.dials == 1
    # The dead stream was discarded and exactly one replacement was proven.
    assert len(harness.streams) == 2
    assert [event.event for event in connection.events][-1] == "stream_reproven"
    assert connection.events[-1].reason == "stale_pooled_stream"
    identity_requests = [
        request
        for request in harness.requests
        if cast(str, request["path"]).startswith("/session-identity")
    ]
    # One identity proof for the original bring-up stream, one for the
    # replacement -- the reconnect RE-PROVES identity on the new stream, it
    # never skips or weakens the proof to retry faster.
    assert len(identity_requests) == 2
    assert all("Authorization" not in cast(dict[str, str], r["headers"]) for r in identity_requests)


def test_stale_pooled_stream_persistent_failure_surfaces_after_exactly_one_reconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second consecutive failure, on the freshly proven stream, surfaces unchanged."""
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    original_getresponse = _Stream.getresponse

    def always_dead(self: _Stream) -> _Response:
        path = cast(str, self.harness.requests[-1]["path"])
        if path == "/jobs/job_1/status":
            raise ConnectionResetError(
                "[WinError 10054] An existing connection was forcibly closed by the remote host"
            )
        return original_getresponse(self)

    monkeypatch.setattr(_Stream, "getresponse", always_dead)

    with pytest.raises(RelayError, match="identity-bound request failed"):
        connection.request_json(method="GET", path="/jobs/job_1/status")

    # Still never escalates to a channel-level reconnect (no new SSH dial), and
    # exactly one stream-level reconnect was attempted -- not an unbounded retry.
    assert harness.dials == 1
    assert len(harness.streams) == 2
    events = [event.event for event in connection.events]
    assert events.count("stream_reproven") == 1
    assert connection.events[-1].reason == "stale_pooled_stream"


def test_a_non_stale_failure_never_triggers_a_stream_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retry is deliberately narrow: an HTTP-status failure is not a stale stream."""
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    original_getresponse = _Stream.getresponse

    def not_found(self: _Stream) -> _Response:
        path = cast(str, self.harness.requests[-1]["path"])
        if path == "/jobs/job_1/status":
            return _Response({"error": "not found"}, status=404)
        return original_getresponse(self)

    monkeypatch.setattr(_Stream, "getresponse", not_found)

    with pytest.raises(RelayError, match="HTTP 404"):
        connection.request_json(method="GET", path="/jobs/job_1/status")

    assert harness.dials == 1
    assert len(harness.streams) == 1
    assert [event.event for event in connection.events][-1] == "established"


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
    script = "set -euo pipefail\nfoo 'bar baz'\nexec cat >/dev/null\n"
    transport = SshForwardTransport(
        definition=_definition(),
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=8765,
        bootstrap_script=script,
    )

    argv = transport.argv(local_port=18_795)

    assert argv[:2] == ["ssh", "-T"]
    assert "-L" in argv
    assert argv[argv.index("-L") + 1] == "127.0.0.1:18795:127.0.0.1:8765"
    assert argv[-2] == "ares-login"

    # ssh joins its trailing operands with spaces into ONE remote command
    # string, so the script must arrive already quoted. Reconstruct what the
    # remote login shell is actually handed and prove it re-parses to exactly
    # `bash -lc <script>` -- an unquoted script would lose `set -euo pipefail`
    # and be interpreted by whatever login shell the account happens to have.
    remote_command = argv[-1]
    assert shlex.split(remote_command) == ["bash", "-lc", script]
    assert "\n" not in remote_command.split(" ", 2)[0]
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
def test_unconfigured_frp_modes_refuse_instead_of_falling_back_to_ssh(mode: str) -> None:
    """A cluster that never opted into the identity anchor must never fall back to SSH.

    #231 R5: brokered_tcp/udp_rendezvous are implemented, but §8.3 requires a
    cluster to explicitly opt into the weaker preshared-link identity anchor
    before either is used. ``_definition()`` here declares neither, so this
    must refuse with the typed anchor error -- never quietly serve the
    connection over ssh_forward or any other substituted mode.
    """
    with pytest.raises(TransportIdentityAnchorRequired, match=mode):
        build_transport(
            mode=cast(Any, mode),
            definition=_definition(),
            session_id="desktop-session-1",
            session_generation_id="generation-1",
            remote_api_port=8765,
            nonce=NONCE,
            api_token="owner-api-token",
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


def test_bring_up_survives_a_login_banner_and_pretty_printed_executor_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bootstrap is framed, not positional.

    ``bash -lc`` runs a login shell whose profile commonly writes a banner to
    stdout, and ``session recovery-status`` pretty-prints its JSON across many
    lines. Neither may break bring-up.
    """
    harness = _Harness()
    _install(monkeypatch, harness)

    def noisy_factory(argv: list[str], **_kwargs: object) -> _ChannelProcess:
        process = _ChannelProcess(
            harness.bootstrap(),
            argv,
            banner=b"Welcome to ares\nLast login: Tue\n\n",
            indent=2,
        )
        harness.processes.append(process)
        return process

    monkeypatch.setattr("clio_relay.control_channel.spawn_channel_process", noisy_factory)

    connection = _connect(tmp_path, harness)

    assert connection.connected is True
    assert harness.dials == 1
    bootstrap = connection.bootstrap
    assert bootstrap is not None
    assert bootstrap.status["session_generation_id"] == "generation-1"


def test_bring_up_without_the_framed_document_fails_with_a_typed_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bring-up whose executors failed must not be mistaken for a good one."""
    harness = _Harness()
    _install(monkeypatch, harness)

    def failing_factory(argv: list[str], **_kwargs: object) -> _ChannelProcess:
        process = _ChannelProcess(harness.bootstrap(), argv)
        process.stdout = io.BytesIO(b"Welcome to ares\n")
        process.stderr = io.BytesIO(b"clio-relay: owned session metadata is unavailable\n")
        process.returncode = 1
        harness.processes.append(process)
        return process

    monkeypatch.setattr("clio_relay.control_channel.spawn_channel_process", failing_factory)

    with pytest.raises(ChannelBootstrapError, match="did not report its bootstrap"):
        _connect(tmp_path, harness)

    assert harness.dials == 1
    assert harness.streams == []


def test_close_escalates_to_terminate_and_kill_when_the_process_ignores_the_pipe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing stdin is the polite teardown; it must not be the only one."""
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    process = harness.processes[0]
    process.ignores_stdin_close = True

    connection.close()

    assert process.terminated is True
    assert process.killed is True
    assert process.poll() is not None


def test_a_stream_proven_against_a_replaced_channel_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stream must never outlive the link it was proven against."""
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)
    # Empty the pool so the next acquire has to prove a new stream.
    harness.streams[0].close()
    connection._idle_streams.clear()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    original_open = remote_connection._open_identity_bound_stream  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    replaced_once = False

    def replace_channel_midway(**kwargs: Any) -> Any:
        nonlocal replaced_once
        if not replaced_once:
            # The channel is replaced while this stream is being proven. Only
            # do it on the first prove, or the reconnect recurses into itself.
            replaced_once = True
            harness.processes[0].drop()
            connection.reconnect()
        return original_open(**kwargs)

    monkeypatch.setattr(
        remote_connection,
        "_open_identity_bound_stream",
        replace_channel_midway,
    )

    with pytest.raises(ChannelDropped, match="replaced while"):
        connection.request_json(method="GET", path="/jobs/job_1/status")


def test_bring_up_diagnostics_do_not_block_on_a_live_process_pipe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failure path must not hang, and a chatty channel must not wedge.

    Uses a real child process, because the hazards are properties of OS pipes:
    a fixed-size read on a live process's stderr blocks until EOF, and an
    undrained stderr pipe fills and stops the writer -- which for a real ``ssh``
    means the port forward stops being serviced, silently.
    """
    harness = _Harness()
    _install(monkeypatch, harness)
    # Writes far more than any pipe buffer to stderr, emits no bootstrap, and
    # never exits on its own.
    child_source = (
        "import sys, time\n"
        "for index in range(20000):\n"
        "    sys.stderr.write('channel %d: open failed: connect failed\n' % index)\n"
        "sys.stderr.flush()\n"
        "time.sleep(600)\n"
    )
    spawned: list[subprocess.Popen[bytes]] = []

    def real_child_factory(_argv: list[str], **kwargs: object) -> subprocess.Popen[bytes]:
        del kwargs
        process = subprocess.Popen(
            [sys.executable, "-c", child_source],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        spawned.append(process)
        harness.processes.append(cast(Any, process))
        return process

    monkeypatch.setattr("clio_relay.control_channel.spawn_channel_process", real_child_factory)

    transport = SshForwardTransport(
        definition=_definition(),
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=DEFAULT_OWNED_SESSION_API_PORT,
        bootstrap_script="true",
        process_factory=real_child_factory,
        ready_timeout_seconds=2.0,
        authorization_timeout_seconds=2.0,
    )
    started = time.monotonic()
    try:
        with pytest.raises(ChannelBootstrapError, match="did not report its bootstrap"):
            transport.establish(nonce=NONCE)
    finally:
        transport.close()
        for process in spawned:
            if process.poll() is None:  # pragma: no cover - defensive cleanup
                process.kill()
                process.wait(timeout=10)
    elapsed = time.monotonic() - started

    # Without the stderr pump the child wedges on a full pipe; without a
    # non-blocking drain the failure path never returns at all.
    assert elapsed < 30
    assert spawned[0].poll() is not None


def test_the_link_is_shaped_for_stream_channels_and_refuses_to_dial_for_them() -> None:
    """Live service streams must ride the one link, never a second one.

    The compute node on a real HPC cluster has no route to the internet, so the
    only viable path is compute node to cluster relay to this link. Nothing
    implements it yet; what must hold now is that the link's shape admits it and
    that asking refuses rather than opening new transport.
    """
    transport = SshForwardTransport(
        definition=_definition(),
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=8765,
        bootstrap_script="true",
    )

    with pytest.raises(StreamChannelsUnavailable, match="one held link"):
        transport.open_stream_channel(name="paraview", remote_port=11111)


def test_established_link_reports_its_control_endpoint_and_stream_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install(monkeypatch, _Harness())
    connection = _connect(tmp_path, harness)

    link = connection.link
    assert link is not None
    assert link.control_endpoint.host == "127.0.0.1"
    assert link.control_endpoint.base_url.startswith("http://127.0.0.1:")
    assert link.bootstrap.status["session_id"] == "desktop-session-1"
    # No mode carries multiplexed stream channels yet; the flag says so rather
    # than the capability being absent from the interface.
    assert link.stream_channels is False

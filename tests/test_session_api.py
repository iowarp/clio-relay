from __future__ import annotations

import http.client
import io
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.control_channel import CHANNEL_BOOTSTRAP_BEGIN, CHANNEL_BOOTSTRAP_END
from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.job_identity import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
)
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    REGISTERED_JARVIS_USER_CONTRACT,
    JobKind,
    McpAdmissionAuthority,
    McpAdmissionClass,
    McpCallSpec,
    McpOperation,
    RelayJob,
    prepare_owned_jarvis_run_submission,
)
from clio_relay.remote_connection import RemoteConnectionRegistry
from clio_relay.session_api import (
    OwnedSessionApiClient,
    session_identity_document,
    submit_owned_session_job,
)


class _Response:
    def __init__(
        self,
        document: object,
        *,
        status: int = 200,
        will_close: bool = False,
    ) -> None:
        self._payload = json.dumps(document).encode("utf-8")
        self.status = status
        self.will_close = will_close

    def read(self, _amount: int) -> bytes:
        return self._payload


class _TimeoutResponse(_Response):
    def read(self, _amount: int) -> bytes:
        raise TimeoutError("bounded response observation expired")


class _Socket:
    def __init__(self, timeout: float | None) -> None:
        self.timeout = timeout
        self.timeout_changes: list[float | None] = []

    def gettimeout(self) -> float | None:
        return self.timeout

    def settimeout(self, timeout: float | None) -> None:
        self.timeout = timeout
        self.timeout_changes.append(timeout)


class _Connection:
    def __init__(
        self,
        responses: list[_Response],
        captured: list[dict[str, object]],
        *,
        fail_authenticated_request: bool = False,
        timeout: float = 30.0,
    ) -> None:
        self._responses = responses
        self._captured = captured
        self._fail_authenticated_request = fail_authenticated_request
        self.auto_open = 1
        self.sock: object | None = None
        self.timeout = timeout
        self.socket = _Socket(timeout)
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
        request_headers = dict(headers or {})
        self._captured.append(
            {
                "method": method,
                "path": path,
                "body": body,
                "headers": request_headers,
                "auto_open": self.auto_open,
            }
        )
        if self._fail_authenticated_request and "Authorization" in request_headers:
            self.sock = None
            raise http.client.NotConnected("identity-proven connection was replaced")

    def getresponse(self) -> _Response:
        return self._responses.pop(0)

    def close(self) -> None:
        self.closed = True
        self.sock = None


class _ChannelProcess:
    """One fake held-channel process standing in for the bring-up SSH dial."""

    def __init__(self, bootstrap: dict[str, object]) -> None:
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO(
            CHANNEL_BOOTSTRAP_BEGIN
            + b"\n"
            + json.dumps(bootstrap).encode("utf-8")
            + b"\n"
            + CHANNEL_BOOTSTRAP_END
            + b"\n"
        )
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
        """Simulate the held channel dying without a local close."""
        self.returncode = 255


def _bootstrap_document(
    *,
    identity: dict[str, str],
    session_generation_id: str = "generation-1",
    remote_api_port: int = 8765,
) -> dict[str, object]:
    """Return the exact out-of-band document one bring-up dial reports."""
    return {
        "schema_version": "clio-relay.channel-bootstrap.v1",
        "status": {
            "owner": "clio-relay",
            "cluster": "ares",
            "session_id": "desktop-session-1",
            "session_generation_id": session_generation_id,
            "remote_api_port": remote_api_port,
            "running": True,
            "ownership_verified": True,
        },
        "identity": dict(identity),
    }


def _settings(tmp_path: Path) -> RelaySettings:
    return RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="ares",
    )


def _job(expected_digest: str) -> RelayJob:
    return RelayJob(
        cluster="ares",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server="clio-kit",
            server_args=["mcp-server", "jarvis"],
            expected_server_artifact_digest=expected_digest,
            admission_class=McpAdmissionClass.CONTROL_QUERY,
            tool="jarvis_get_execution",
            arguments={"execution_id": "execution-1"},
            timeout_seconds=60,
        ),
        idempotency_key="owned-session-client",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        metadata={
            "owner": "clio-relay",
            "owner_session_id": "desktop-session-1",
            "owner_session_generation_id": "generation-1",
            MCP_ADMISSION_AUTHORITY_METADATA_KEY: McpAdmissionAuthority(
                source="pinned_jarvis_contract",
                operation=McpOperation.TOOLS_CALL,
                tool="jarvis_get_execution",
                expected_server_artifact_digest=expected_digest,
            ).model_dump(mode="json"),
        },
    )


def _admitted_jarvis_run_job(
    expected_digest: str,
    *,
    pipeline_id: str = "science-pipeline",
) -> RelayJob:
    """Return the exact server-normalized receipt for an owned JARVIS run."""

    submitted = RelayJob(
        cluster="ares",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server="clio-kit",
            server_args=["mcp-server", "jarvis"],
            expected_server_artifact_digest=expected_digest,
            expected_jarvis_cd_lock_binding={
                "schema_version": "clio-relay.jarvis-cd-lock-expectation.v1",
                "version": "1.3.16",
                "url": "https://example.invalid/jarvis-cd.whl",
                "sha256": "b" * 64,
            },
            tool="jarvis_run",
            arguments={"pipeline_id": pipeline_id},
        ),
        idempotency_key="owned-session-client",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        metadata={
            "owner": "clio-relay",
            "owner_session_id": "desktop-session-1",
            "owner_session_generation_id": "generation-1",
        },
    )
    return prepare_owned_jarvis_run_submission(submitted)


def _install_transport(
    monkeypatch: pytest.MonkeyPatch,
    *,
    responses: list[_Response],
    fail_authenticated_request: bool = False,
    bootstrap: dict[str, object] | None = None,
) -> _Transport:
    """Install one fake held channel and expose its dial and request record.

    Every element of the real transport is replaced at the seam the production
    code actually uses: the channel process factory (one call == one SSH dial),
    the loopback health probe, and the proven HTTP stream.  Nothing here can
    reach a real ``ssh`` process, so a dial count is exact.
    """
    nonce = "1" * 64
    expected_identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce=nonce,
    )
    document = bootstrap or _bootstrap_document(identity=expected_identity)

    captured: list[dict[str, object]] = []
    connections: list[_Connection] = []
    processes: list[_ChannelProcess] = []

    def connection_factory(*_args: object, **kwargs: object) -> _Connection:
        timeout_value = kwargs.get("timeout", 30.0)
        if isinstance(timeout_value, bool) or not isinstance(timeout_value, (int, float)):
            raise AssertionError("HTTP connection timeout must be numeric")
        connection = _Connection(
            responses,
            captured,
            fail_authenticated_request=fail_authenticated_request,
            timeout=float(timeout_value),
        )
        connections.append(connection)
        return connection

    def process_factory(*_args: object, **_kwargs: object) -> _ChannelProcess:
        process = _ChannelProcess(document)
        processes.append(process)
        return process

    def token_hex(_size: int) -> str:
        return nonce

    registry = RemoteConnectionRegistry()

    def skip_health(*_args: object, **_kwargs: object) -> None:
        """The forward is faked, so its loopback readiness probe is a no-op."""

    monkeypatch.setattr("clio_relay.control_channel.spawn_channel_process", process_factory)
    monkeypatch.setattr("clio_relay.control_channel._wait_for_channel_health", skip_health)
    monkeypatch.setattr("clio_relay.remote_connection.secrets.token_hex", token_hex)
    monkeypatch.setattr(
        "clio_relay.remote_connection.http.client.HTTPConnection",
        connection_factory,
    )
    monkeypatch.setattr("clio_relay.session_api.connection_registry", lambda: registry)
    return _Transport(
        captured=captured,
        connections=connections,
        processes=processes,
        registry=registry,
    )


@dataclass
class _Transport:
    """The observable record of one installed fake channel."""

    captured: list[dict[str, object]]
    connections: list[_Connection]
    processes: list[_ChannelProcess]
    registry: RemoteConnectionRegistry

    @property
    def dials(self) -> int:
        """Return how many new channel processes (SSH dials) were spawned."""
        return len(self.processes)


def _submit_jarvis_run_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    admitted: RelayJob,
    expected_digest: str,
    arguments: dict[str, object],
) -> RelayJob:
    """Submit caller arguments against one simulated authenticated admission receipt."""

    identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce="1" * 64,
    )
    _install_transport(
        monkeypatch,
        responses=[
            _Response(identity),
            _Response(admitted.model_dump(mode="json")),
        ],
    )
    return submit_owned_session_job(
        definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
        settings=_settings(tmp_path),
        path="/jobs/jarvis-mcp-call",
        payload={
            "cluster": "ares",
            "tool": "jarvis_run",
            "arguments": arguments,
            "expected_server_artifact_digest": expected_digest,
            "idempotency_key": "owned-session-client",
        },
    )


def test_owned_session_client_proves_identity_before_sending_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_digest = "a" * 64
    identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce="1" * 64,
    )
    transport = _install_transport(
        monkeypatch,
        responses=[
            _Response(identity),
            _Response(_job(expected_digest).model_dump(mode="json")),
        ],
    )
    payload: dict[str, object] = {
        "cluster": "ares",
        "tool": "jarvis_get_execution",
        "arguments": {"execution_id": "execution-1"},
        "expected_server_artifact_digest": expected_digest,
        "timeout_seconds": 60,
        "idempotency_key": "owned-session-client",
    }

    job = submit_owned_session_job(
        definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
        settings=_settings(tmp_path),
        path="/jobs/jarvis-mcp-call",
        payload=payload,
    )

    assert job.owner_session_id == "desktop-session-1"
    assert job.owner_session_generation_id == "generation-1"
    assert job.metadata["owner_session_generation_id"] == "generation-1"
    assert len(transport.connections) == 1
    proof_headers = transport.captured[0]["headers"]
    assert isinstance(proof_headers, dict)
    assert "Authorization" not in proof_headers
    assert OWNER_SESSION_ID_HEADER not in proof_headers
    assert SESSION_GENERATION_ID_HEADER not in proof_headers
    assert transport.captured[0]["path"] == f"/session-identity?nonce={'1' * 64}"
    auth_headers = transport.captured[1]["headers"]
    assert isinstance(auth_headers, dict)
    assert auth_headers["Authorization"] == "Bearer session-api-token"
    assert auth_headers[OWNER_SESSION_ID_HEADER] == "desktop-session-1"
    assert auth_headers[SESSION_GENERATION_ID_HEADER] == "generation-1"
    assert transport.captured[1]["auto_open"] == 0


def test_owned_session_submission_rejects_dropped_artifact_dependencies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_digest = "a" * 64
    identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce="1" * 64,
    )
    _install_transport(
        monkeypatch,
        responses=[
            _Response(identity),
            _Response(_job(expected_digest).model_dump(mode="json")),
        ],
    )

    with pytest.raises(RelayError, match="did not retain exact artifact dependencies"):
        submit_owned_session_job(
            definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
            settings=_settings(tmp_path),
            path="/jobs/jarvis-mcp-call",
            payload={
                "cluster": "ares",
                "tool": "jarvis_get_execution",
                "arguments": {"execution_id": "execution-1"},
                "expected_server_artifact_digest": expected_digest,
                "idempotency_key": "owned-session-client",
                "used_artifact_refs": [{"artifact_id": "artifact_input", "sha256": "b" * 64}],
            },
        )


def test_owned_session_submission_accepts_relay_owned_jarvis_execution_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Admission may add only its deterministic execution ID to jarvis_run arguments."""

    expected_digest = "a" * 64
    admitted = _admitted_jarvis_run_job(expected_digest)
    received = _submit_jarvis_run_receipt(
        tmp_path,
        monkeypatch,
        admitted=admitted,
        expected_digest=expected_digest,
        arguments={"pipeline_id": "science-pipeline"},
    )

    assert isinstance(received.spec, McpCallSpec)
    assert isinstance(admitted.spec, McpCallSpec)
    assert received.spec.arguments == admitted.spec.arguments
    assert received.spec.arguments["execution_id"].startswith("jarvis_")


def test_owned_session_submission_accepts_registered_jarvis_execution_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registered JARVIS admission may add its deterministic execution ID."""

    expected_digest = "a" * 64
    submitted = RelayJob(
        cluster="ares",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server="clio-kit",
            server_args=["mcp-server", "jarvis"],
            expected_server_artifact_digest=expected_digest,
            expected_registered_contract=REGISTERED_JARVIS_USER_CONTRACT,
            tool="jarvis_run",
            arguments={"pipeline_id": "science-pipeline"},
        ),
        idempotency_key="owned-session-client",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        metadata={
            "owner": "clio-relay",
            "owner_session_id": "desktop-session-1",
            "owner_session_generation_id": "generation-1",
        },
    )
    admitted = prepare_owned_jarvis_run_submission(submitted)
    identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce="1" * 64,
    )
    _install_transport(
        monkeypatch,
        responses=[
            _Response(identity),
            _Response(admitted.model_dump(mode="json")),
        ],
    )

    received = submit_owned_session_job(
        definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
        settings=_settings(tmp_path),
        path="/jobs/mcp-call",
        payload={
            "cluster": "ares",
            "server": "clio-kit",
            "server_args": ["mcp-server", "jarvis"],
            "expected_server_artifact_digest": expected_digest,
            "expected_registered_contract": REGISTERED_JARVIS_USER_CONTRACT,
            "tool": "jarvis_run",
            "arguments": {"pipeline_id": "science-pipeline"},
            "idempotency_key": "owned-session-client",
        },
    )

    assert isinstance(received.spec, McpCallSpec)
    assert received.spec.arguments["execution_id"].startswith("jarvis_")


def test_owned_session_submission_rejects_other_jarvis_run_arguments_after_normalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Execution-ID normalization must not hide a changed pipeline or other caller input."""

    expected_digest = "a" * 64
    admitted = _admitted_jarvis_run_job(expected_digest, pipeline_id="other-pipeline")
    with pytest.raises(RelayError, match="different JARVIS MCP call"):
        _submit_jarvis_run_receipt(
            tmp_path,
            monkeypatch,
            admitted=admitted,
            expected_digest=expected_digest,
            arguments={"pipeline_id": "science-pipeline"},
        )


def test_owned_session_submission_accepts_only_proven_jarvis_execution_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit execution ID is valid only when the receipt's durable identity proves it."""

    expected_digest = "a" * 64
    admitted = _admitted_jarvis_run_job(expected_digest)
    assert isinstance(admitted.spec, McpCallSpec)
    execution_id = admitted.spec.arguments["execution_id"]
    received = _submit_jarvis_run_receipt(
        tmp_path,
        monkeypatch,
        admitted=admitted,
        expected_digest=expected_digest,
        arguments={
            "pipeline_id": "science-pipeline",
            "execution_id": execution_id,
        },
    )
    assert isinstance(received.spec, McpCallSpec)
    assert received.spec.arguments["execution_id"] == execution_id

    with pytest.raises(RelayError, match="different execution identity"):
        _submit_jarvis_run_receipt(
            tmp_path,
            monkeypatch,
            admitted=admitted,
            expected_digest=expected_digest,
            arguments={
                "pipeline_id": "science-pipeline",
                "execution_id": "jarvis_forged",
            },
        )


def test_owned_session_client_reuses_one_proven_connection_for_composite_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce="1" * 64,
    )
    transport = _install_transport(
        monkeypatch,
        responses=[_Response(identity), _Response({"state": "running"}), _Response({"ok": True})],
    )

    with OwnedSessionApiClient(
        definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
        settings=_settings(tmp_path),
    ) as client:
        assert client.request_json(method="GET", path="/jobs/job_1/status") == {"state": "running"}
        assert client.request_json(
            method="GET", path="/jobs/job_1/logs/stdout", query={"offset": 0, "limit": 64}
        ) == {"ok": True}

    assert len(transport.connections) == 1
    assert [request["method"] for request in transport.captured] == ["GET", "GET", "GET"]


def test_owned_session_client_scopes_long_response_timeout_to_one_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce="1" * 64,
    )
    transport = _install_transport(
        monkeypatch,
        responses=[_Response(identity), _Response({"state": "succeeded"}), _Response({})],
    )

    with OwnedSessionApiClient(
        definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
        settings=_settings(tmp_path),
    ) as client:
        assert client.request_json(
            method="POST",
            path="/jobs/job_1/wait",
            response_timeout_seconds=610,
        ) == {"state": "succeeded"}
        assert transport.connections[0].timeout == 30
        assert transport.connections[0].socket.timeout == 30
        assert client.request_json(method="GET", path="/jobs/job_1/status") == {}

    assert len(transport.connections) == 1
    assert transport.connections[0].socket.timeout_changes == [610, 30]


def test_owned_session_client_types_only_a_bounded_response_deadline_as_observation_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce="1" * 64,
    )
    transport = _install_transport(
        monkeypatch,
        responses=[_Response(identity), _TimeoutResponse({})],
    )

    with (
        OwnedSessionApiClient(
            definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
            settings=_settings(tmp_path),
        ) as client,
        pytest.raises(ObservationTimeoutError, match="response observation timed out"),
    ):
        client.request_json(
            method="POST",
            path="/jobs/job_1/wait",
            response_timeout_seconds=0.25,
        )

    assert transport.connections[0].socket.timeout_changes == [0.25, 30]


def test_owned_session_client_never_reconnects_after_identity_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce="1" * 64,
    )
    transport = _install_transport(
        monkeypatch,
        responses=[_Response(identity)],
        fail_authenticated_request=True,
    )

    with (
        pytest.raises(RelayError, match="identity-bound request failed"),
        OwnedSessionApiClient(
            definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
            settings=_settings(tmp_path),
        ) as client,
    ):
        client.request_json(method="GET", path="/jobs/job_1/status")

    assert len(transport.connections) == 1
    assert transport.captured[1]["auto_open"] == 0


def test_owned_session_client_rejects_replaced_generation_before_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A channel that reaches a replaced generation never carries a credential."""
    identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-2",
        nonce="1" * 64,
    )
    transport = _install_transport(
        monkeypatch,
        responses=[_Response(identity)],
        bootstrap=_bootstrap_document(identity=identity, session_generation_id="generation-2"),
    )

    with pytest.raises(RelayError, match="ownership-verified generation"):
        submit_owned_session_job(
            definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
            settings=_settings(tmp_path),
            path="/jobs/jarvis-mcp-call",
            payload={
                "cluster": "ares",
                "tool": "jarvis_get_execution",
                "arguments": {},
                "expected_server_artifact_digest": "a" * 64,
                "idempotency_key": "stale-generation",
            },
        )

    assert transport.dials == 1
    assert transport.connections == []
    assert transport.captured == []
    assert transport.processes[0].poll() is not None

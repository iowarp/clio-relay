from __future__ import annotations

import http.client
import json
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.errors import RelayError
from clio_relay.models import JobKind, McpCallSpec, RelayJob
from clio_relay.session_api import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
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


class _Connection:
    def __init__(
        self,
        responses: list[_Response],
        captured: list[dict[str, object]],
        *,
        fail_authenticated_request: bool = False,
    ) -> None:
        self._responses = responses
        self._captured = captured
        self._fail_authenticated_request = fail_authenticated_request
        self.auto_open = 1
        self.sock: object | None = None
        self.closed = False

    def connect(self) -> None:
        self.sock = object()

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


class _ReadinessClient:
    def __enter__(self) -> _ReadinessClient:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


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
            tool="jarvis_get_execution",
            arguments={"execution_id": "execution-1"},
        ),
        idempotency_key="owned-session-client",
        metadata={
            "owner": "clio-relay",
            "owner_session_id": "desktop-session-1",
            "owner_session_generation_id": "generation-1",
        },
    )


def _install_transport(
    monkeypatch: pytest.MonkeyPatch,
    *,
    responses: list[_Response],
    fail_authenticated_request: bool = False,
) -> tuple[list[dict[str, object]], list[_Connection]]:
    nonce = "1" * 64

    def status(*, definition: ClusterDefinition, session_id: str) -> dict[str, object]:
        assert definition.name == "ares"
        assert session_id == "desktop-session-1"
        return {
            "owner": "clio-relay",
            "cluster": "ares",
            "session_id": "desktop-session-1",
            "session_generation_id": "generation-1",
            "remote_api_port": 8766,
            "running": True,
            "ownership_verified": True,
        }

    expected_identity = session_identity_document(
        owner_token="owner-token",
        cluster="ares",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce=nonce,
    )

    def challenge(**_kwargs: object) -> dict[str, object]:
        return dict(expected_identity)

    captured: list[dict[str, object]] = []
    connections: list[_Connection] = []

    def connection_factory(*_args: object, **_kwargs: object) -> _Connection:
        connection = _Connection(
            responses,
            captured,
            fail_authenticated_request=fail_authenticated_request,
        )
        connections.append(connection)
        return connection

    @contextmanager
    def forward(**_kwargs: Any) -> Generator[int, None, None]:
        yield 18_766

    def token_hex(_size: int) -> str:
        return nonce

    def readiness_client(**_kwargs: object) -> _ReadinessClient:
        return _ReadinessClient()

    monkeypatch.setattr("clio_relay.session_api.status_remote_session", status)
    monkeypatch.setattr("clio_relay.session_api.challenge_remote_session_identity", challenge)
    monkeypatch.setattr("clio_relay.session_api.secrets.token_hex", token_hex)
    monkeypatch.setattr("clio_relay.session_api.httpx.Client", readiness_client)
    monkeypatch.setattr("clio_relay.session_api.http.client.HTTPConnection", connection_factory)
    monkeypatch.setattr("clio_relay.session_api._ssh_forward", forward)
    return captured, connections


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
    captured, connections = _install_transport(
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
        "idempotency_key": "owned-session-client",
    }

    job = submit_owned_session_job(
        definition=ClusterDefinition(name="ares", ssh_host="ares-login"),
        settings=_settings(tmp_path),
        path="/jobs/jarvis-mcp-call",
        payload=payload,
    )

    assert job.metadata["owner_session_generation_id"] == "generation-1"
    assert len(connections) == 1
    proof_headers = captured[0]["headers"]
    assert isinstance(proof_headers, dict)
    assert "Authorization" not in proof_headers
    assert OWNER_SESSION_ID_HEADER not in proof_headers
    assert SESSION_GENERATION_ID_HEADER not in proof_headers
    assert captured[0]["path"] == f"/session-identity?nonce={'1' * 64}"
    auth_headers = captured[1]["headers"]
    assert isinstance(auth_headers, dict)
    assert auth_headers["Authorization"] == "Bearer session-api-token"
    assert auth_headers[OWNER_SESSION_ID_HEADER] == "desktop-session-1"
    assert auth_headers[SESSION_GENERATION_ID_HEADER] == "generation-1"
    assert captured[1]["auto_open"] == 0


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
    captured, connections = _install_transport(
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

    assert len(connections) == 1
    assert [request["method"] for request in captured] == ["GET", "GET", "GET"]


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
    captured, connections = _install_transport(
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

    assert len(connections) == 1
    assert captured[1]["auto_open"] == 0


def test_owned_session_client_rejects_replaced_generation_before_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def status(*, definition: ClusterDefinition, session_id: str) -> dict[str, object]:
        del definition, session_id
        return {
            "owner": "clio-relay",
            "cluster": "ares",
            "session_id": "desktop-session-1",
            "session_generation_id": "generation-2",
            "remote_api_port": 8766,
            "running": True,
            "ownership_verified": True,
        }

    def fail_client(**_kwargs: object) -> _ReadinessClient:
        raise AssertionError("stale generation opened an HTTP transport")

    monkeypatch.setattr("clio_relay.session_api.status_remote_session", status)
    monkeypatch.setattr("clio_relay.session_api.httpx.Client", fail_client)

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

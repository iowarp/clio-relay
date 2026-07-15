"""Focused coverage for public gateway credential redaction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from clio_relay import cli as relay_cli
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.http_api import create_app
from clio_relay.mcp_server import handle_request
from clio_relay.models import GatewaySession
from clio_relay.service_runtime import ServiceRuntimeStopResult
from clio_relay.session_lifecycle import CleanupResource

_OWNER_TOKEN = "owner-capability-8c699a93d7cc4af1"
_API_TOKEN = "api-capability-69dc7b420af54a1b"
_BROWSER_TOKEN = "browser-capability-1e3f2fc84936402a"
_SECRET_KEY = "shared-secret-d8aeef8970aa47be"
_PASSWORD = "password-46e9d94af8fb4368"
_PRIVATE_KEY = "private-key-b2f3a5f9818f46a8"
_CREDENTIAL = "credential-014e1215a0664320"
_SENSITIVE_VALUES = (
    _OWNER_TOKEN,
    _API_TOKEN,
    _BROWSER_TOKEN,
    _SECRET_KEY,
    _PASSWORD,
    _PRIVATE_KEY,
    _CREDENTIAL,
)


def _gateway_with_credentials() -> GatewaySession:
    """Build a gateway whose credentials appear at varied depths and in diagnostics."""
    return GatewaySession(
        cluster="test-cluster",
        name="owned-runtime",
        requested_resources={
            "bootstrap": {
                "credential": _CREDENTIAL,
                "diagnostic": f"credential={_CREDENTIAL}",
            }
        },
        gateway={
            "transport": {
                "desktop_connector": {
                    "owner_token": _OWNER_TOKEN,
                    "command": f"connector --owner-token={_OWNER_TOKEN}",
                    "nested": [
                        {
                            "api_token": _API_TOKEN,
                            "message": f"observed {_API_TOKEN}",
                        }
                    ],
                },
                "browser_proxy": {
                    "browser_access_token": _BROWSER_TOKEN,
                    "connect_url": f"http://127.0.0.1/?access={_BROWSER_TOKEN}",
                },
                "secret_key": _SECRET_KEY,
            },
            "ownership_intents": {
                "desktop_connector": {
                    "owner_token": _OWNER_TOKEN,
                    "evidence": f"owner={_OWNER_TOKEN}; secret={_SECRET_KEY}",
                }
            },
            "password": _PASSWORD,
        },
        metadata={
            "owner": "clio-relay",
            "private_key": _PRIVATE_KEY,
            "summary": f"private={_PRIVATE_KEY}; password={_PASSWORD}",
        },
    )


def _is_sensitive_key(key: object) -> bool:
    """Mirror the documented public sensitivity convention for output assertions."""
    if not isinstance(key, str):
        return False
    normalized = key.strip().casefold().replace("-", "_").replace(".", "_")
    return normalized in {
        "authorization",
        "credential",
        "credentials",
        "password",
        "private_key",
        "secret",
        "secret_key",
        "token",
    } or normalized.endswith(
        (
            "_authorization",
            "_credential",
            "_credentials",
            "_password",
            "_private_key",
            "_secret",
            "_secret_key",
            "_token",
        )
    )


def _assert_public_document(value: object) -> None:
    """Assert exhaustive nested key redaction and raw-value absence."""
    rendered = json.dumps(value, sort_keys=True)
    for sensitive in _SENSITIVE_VALUES:
        assert sensitive not in rendered

    def visit(nested: object) -> None:
        if isinstance(nested, dict):
            for key, item in cast(dict[object, object], nested).items():
                if _is_sensitive_key(key):
                    assert item == "<redacted>"
                else:
                    visit(item)
        elif isinstance(nested, list):
            for item in cast(list[object], nested):
                visit(item)

    visit(value)


def _assert_internal_credentials_remain(queue: ClioCoreQueue, session_id: str) -> None:
    """Assert public reads did not mutate the durable ownership record."""
    rendered = queue.get_gateway_session(session_id).model_dump_json()
    for sensitive in _SENSITIVE_VALUES:
        assert sensitive in rendered


def test_cli_gateway_list_and_get_redact_nested_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Routine local CLI reads expose only the public gateway projection."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    queue = ClioCoreQueue(tmp_path / "core")
    session = queue.create_gateway_session(_gateway_with_credentials())

    listed = CliRunner().invoke(app, ["gateway", "list", "--cluster", session.cluster])
    read = CliRunner().invoke(app, ["gateway", "get", session.session_id])

    assert listed.exit_code == 0, listed.output
    assert read.exit_code == 0, read.output
    listed_payload = cast(dict[str, Any], json.loads(listed.output))
    listed_sessions = cast(list[object], listed_payload["gateway_sessions"])
    assert len(listed_sessions) == 1
    _assert_public_document(listed_sessions[0])
    _assert_public_document(json.loads(read.output))
    _assert_internal_credentials_remain(queue, session.session_id)


def test_cli_remote_gateway_get_redacts_untrusted_remote_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A desktop CLI does not trust an older remote CLI to redact its gateway record."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    session = _gateway_with_credentials()
    definition = ClusterDefinition(name=session.cluster, ssh_host="cluster-login")

    def require_cluster(_cluster: str) -> ClusterDefinition:
        return definition

    def execute_remotely(_definition: ClusterDefinition) -> bool:
        return True

    def remote_gateway(
        _definition: ClusterDefinition,
        _arguments: list[str],
    ) -> str:
        return session.model_dump_json()

    monkeypatch.setattr(relay_cli, "_require_cluster", require_cluster)
    monkeypatch.setattr(relay_cli, "should_execute_on_cluster", execute_remotely)
    monkeypatch.setattr(relay_cli, "run_remote_clio", remote_gateway)

    read = CliRunner().invoke(
        app,
        ["gateway", "get", session.session_id, "--cluster", session.cluster],
    )

    assert read.exit_code == 0, read.output
    _assert_public_document(json.loads(read.output))


def test_mcp_gateway_list_and_get_redact_nested_credentials(tmp_path: Path) -> None:
    """MCP text and structured results share the same safe gateway projection."""
    queue = ClioCoreQueue(tmp_path / "core")
    session = queue.create_gateway_session(_gateway_with_credentials())
    calls = (
        ("relay_list_gateway_sessions", {"cluster": session.cluster}),
        ("relay_get_gateway_session", {"session_id": session.session_id}),
    )

    for request_id, (name, arguments) in enumerate(calls, start=1):
        response = handle_request(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            },
            queue=queue,
            profile="admin",
        )
        assert response is not None
        assert "error" not in response
        result = cast(dict[str, Any], response["result"])
        structured = result["structuredContent"]
        content = cast(list[dict[str, str]], result["content"])
        text_payload = json.loads(content[0]["text"])
        assert text_payload == structured
        _assert_public_document(structured)
        _assert_public_document(text_payload)

    _assert_internal_credentials_remain(queue, session.session_id)


def test_http_gateway_list_and_get_redact_nested_credentials(tmp_path: Path) -> None:
    """The existing HTTP public-record boundary removes the same nested credentials."""
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = queue.create_gateway_session(_gateway_with_credentials())
    client = cast(Any, TestClient(create_app(settings)))

    listed = client.get("/gateway-sessions", params={"cluster": session.cluster})
    read = client.get(f"/gateway-sessions/{session.session_id}")

    assert listed.status_code == 200
    assert read.status_code == 200
    listed_sessions = cast(list[object], listed.json()["gateway_sessions"])
    assert len(listed_sessions) == 1
    _assert_public_document(listed_sessions[0])
    _assert_public_document(read.json())
    _assert_internal_credentials_remain(queue, session.session_id)


def test_runtime_cleanup_payload_uses_public_gateway_projection() -> None:
    """Detach and stop reports cannot reintroduce the durable gateway capabilities."""
    session = _gateway_with_credentials()
    result = ServiceRuntimeStopResult(
        session=session,
        mode="detach",
        stopped_local_pid=None,
        stopped_remote_pid=None,
        canceled_scheduler_job=None,
        resources=[
            CleanupResource(
                kind="desktop_connector",
                resource_id="connector-1",
                location="desktop",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                metadata={
                    "owner_token": _OWNER_TOKEN,
                    "diagnostic": f"stopped owner {_OWNER_TOKEN}",
                },
            )
        ],
        errors=[f"historic diagnostic mentioned {_OWNER_TOKEN}"],
    )

    _assert_public_document(result.json_payload())
    for sensitive in _SENSITIVE_VALUES:
        assert sensitive in session.model_dump_json()

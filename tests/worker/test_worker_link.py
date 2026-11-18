"""The worker's link checks, without a site: identity, site API and refusals.

The live conformance module covers the same paths against a placed worker.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.worker.adapter import (
    WORKER_LINK_FILE,
    WorkerAdapter,
    WorkerConfig,
    WorkerLink,
)
from clio_relay.adapters.worker.link import WORKER_PROTOCOL
from clio_relay.adapters.worker.site_app import SiteSettings, site_app
from clio_relay.job_identity import OWNER_SESSION_ID_HEADER, SESSION_GENERATION_ID_HEADER
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.session_api import SESSION_IDENTITY_SCHEMA, session_identity_document

NONCE = "ab" * 32


def _settings(tmp_path: Path) -> SiteSettings:
    return SiteSettings(
        cluster="worker-site",
        session_id="session_1",
        generation_id="generation_1",
        api_token="api-token",
        session_owner_token="owner-token",
        state_dir=tmp_path,
        port=8765,
    )


def _headers(**overrides: str) -> dict[str, str]:
    headers = {
        "Authorization": "Bearer api-token",
        OWNER_SESSION_ID_HEADER: "session_1",
        SESSION_GENERATION_ID_HEADER: "generation_1",
    }
    headers.update(overrides)
    return headers


def test_identity_document_is_an_hmac_over_the_domain_separated_fields() -> None:
    document = session_identity_document(
        owner_token="owner-token",
        cluster="c",
        session_id="s",
        generation_id="g",
        nonce=NONCE,
    )

    message = "\n".join((SESSION_IDENTITY_SCHEMA, "c", "s", "g", NONCE)).encode()
    assert document["hmac_sha256"] == hmac.new(b"owner-token", message, hashlib.sha256).hexdigest()
    assert document["schema_version"] == SESSION_IDENTITY_SCHEMA
    assert document["nonce"] == NONCE


@pytest.mark.parametrize("nonce", ["", "AB" * 32, "ab" * 31, "zz" * 32])
def test_identity_document_refuses_a_nonce_that_is_not_256_bit_lowercase_hex(nonce: str) -> None:
    with pytest.raises(ValueError, match="nonce"):
        session_identity_document(
            owner_token="t", cluster="c", session_id="s", generation_id="g", nonce=nonce
        )


def test_site_answers_the_link_health_probe_without_a_credential(tmp_path: Path) -> None:
    # frp_link.wait_for_channel_health polls /healthz for {"ok": true} before
    # the identity check; without it the link never comes up.
    client = TestClient(site_app(_settings(tmp_path)))

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_site_answers_the_identity_challenge_without_a_credential(tmp_path: Path) -> None:
    client = TestClient(site_app(_settings(tmp_path)))

    response = client.get("/session-identity", params={"nonce": NONCE})

    assert response.status_code == 200
    assert response.json() == session_identity_document(
        owner_token="owner-token",
        cluster="worker-site",
        session_id="session_1",
        generation_id="generation_1",
        nonce=NONCE,
    )
    assert client.get("/session-identity", params={"nonce": "short"}).status_code == 422


def test_site_status_needs_the_bearer_token(tmp_path: Path) -> None:
    client = TestClient(site_app(_settings(tmp_path)))

    assert client.get("/session-status").status_code == 401
    wrong = _headers(Authorization="Bearer not-the-token")
    assert client.get("/session-status", headers=wrong).status_code == 401


def test_site_status_refuses_another_sessions_headers(tmp_path: Path) -> None:
    client = TestClient(site_app(_settings(tmp_path)))

    other_session = _headers(**{OWNER_SESSION_ID_HEADER: "session_2"})
    other_generation = _headers(**{SESSION_GENERATION_ID_HEADER: "generation_2"})
    assert client.get("/session-status", headers=other_session).status_code == 409
    assert client.get("/session-status", headers=other_generation).status_code == 409


def test_site_status_reports_the_placed_session_and_its_protocol(tmp_path: Path) -> None:
    client = TestClient(site_app(_settings(tmp_path)))

    response = client.get("/session-status", headers=_headers())

    assert response.status_code == 200
    status = response.json()
    assert status["worker_protocol"] == WORKER_PROTOCOL
    assert status["session_id"] == "session_1"
    assert status["session_generation_id"] == "generation_1"
    assert status["remote_api_port"] == 8765
    assert status["running"] is True


def _adapter(tmp_path: Path) -> WorkerAdapter:
    context = AdapterContext(
        site_id="site", state_dir=tmp_path / "adapter", content=ContentStore(tmp_path / "content")
    )
    return WorkerAdapter(WorkerConfig(ssh_profile="site-login"), context)


def test_no_stored_link_asks_for_sign_in(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)

    with pytest.raises(Refusal) as refusal:
        asyncio.run(adapter._connection())  # pyright: ignore[reportPrivateUsage]

    assert refusal.value.reason is RefusalReason.NEEDS_SIGN_IN
    assert "sign in to site-login" in refusal.value.message


def test_a_link_stored_by_an_older_relay_asks_for_sign_in_to_replace_the_worker(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path)
    older = WorkerLink(
        cluster="worker-site",
        session_id="session_1",
        generation_id="generation_1",
        api_token="api-token",
        stcp_secret="stcp-secret",
        proxy_name="clio-worker-0",
        port=8765,
        remote_dir="~/.local/share/clio-relay/worker/site",
    )
    adapter.context.state_dir.mkdir(parents=True)
    (adapter.context.state_dir / WORKER_LINK_FILE).write_text(
        older.model_dump_json(exclude={"session_owner_token"}), encoding="utf-8"
    )

    with pytest.raises(Refusal) as refusal:
        asyncio.run(adapter._connection())  # pyright: ignore[reportPrivateUsage]

    assert refusal.value.reason is RefusalReason.NEEDS_SIGN_IN
    assert "older relay" in refusal.value.message

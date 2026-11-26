"""The worker's link checks, without a site: identity, site API and refusals.

The live conformance module covers the same paths against a placed worker.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.worker.adapter import (
    WORKER_LINK_FILE,
    Rendezvous,
    WorkerAdapter,
    WorkerConfig,
    WorkerLink,
)
from clio_relay.adapters.worker.link import WORKER_PROTOCOL
from clio_relay.adapters.worker.site_app import SiteSettings, site_app
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.session_api import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
    SESSION_IDENTITY_SCHEMA,
    session_identity_document,
)

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


def _files(client: TestClient, op: str, **body: object) -> dict[str, Any]:
    response = client.post(
        f"/adapter/files/{op}", headers=_headers(), json={"namespace": "ab" * 8, **body}
    )
    assert response.status_code == 200
    return response.json()


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def test_site_writes_a_file_in_chunks_and_refuses_a_chunk_out_of_place(tmp_path: Path) -> None:
    client = TestClient(site_app(_settings(tmp_path)))

    first = _files(client, "write-chunk", path="d/f.bin", data=_b64(b"abc"), offset=0, final=False)
    assert "ok" in first
    gap = _files(client, "write-chunk", path="d/f.bin", data=_b64(b"x"), offset=5, final=False)
    assert gap["refusal"]["reason"] == RefusalReason.INVALID_ARGUMENTS.value
    last = _files(client, "write-chunk", path="d/f.bin", data=_b64(b"def"), offset=3, final=True)
    assert last["ok"]["size"] == 6
    read = _files(client, "read", path="d/f.bin")
    assert base64.b64decode(read["ok"]) == b"abcdef"


@pytest.mark.parametrize("offset", [True, "3", 1.5])
def test_site_refuses_a_chunk_offset_that_is_not_an_integer(tmp_path: Path, offset: object) -> None:
    client = TestClient(site_app(_settings(tmp_path)))

    answer = _files(client, "write-chunk", path="f", data=_b64(b"x"), offset=offset, final=True)

    assert answer["refusal"]["reason"] == RefusalReason.INVALID_ARGUMENTS.value
    assert not (tmp_path / "adapter" / ("ab" * 8) / "f").exists()


def test_site_deletes_a_file_and_a_directory_only_when_recursive(tmp_path: Path) -> None:
    client = TestClient(site_app(_settings(tmp_path)))
    _files(client, "write", path="d/inner/f", data=_b64(b"x"))
    _files(client, "write", path="g", data=_b64(b"y"))

    assert _files(client, "delete", path="g") == {"ok": None}
    assert "refusal" in _files(client, "delete", path="d")
    assert _files(client, "delete", path="d", recursive=True) == {"ok": None}
    root = tmp_path / "adapter" / ("ab" * 8)
    assert not (root / "g").exists()
    assert not (root / "d").exists()


def test_site_refuses_a_file_operation_it_does_not_know(tmp_path: Path) -> None:
    # An older site answers write-chunk and delete this way, so the client never
    # mistakes one chunk for a whole file.
    client = TestClient(site_app(_settings(tmp_path)))

    answer = _files(client, "truncate", path="f")

    assert answer["refusal"]["reason"] == RefusalReason.INVALID_ARGUMENTS.value
    assert "unknown file operation" in answer["refusal"]["message"]


def _adapter(tmp_path: Path) -> WorkerAdapter:
    context = AdapterContext(
        site_id="site", state_dir=tmp_path / "adapter", content=ContentStore(tmp_path / "content")
    )
    return WorkerAdapter(WorkerConfig(ssh_profile="site-login"), context)


def test_the_worker_reads_the_port_a_package_service_bound_from_the_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Without this a package service on a worker site never learns the port it bound.
    from clio_relay.adapters.local import LocalAdapter
    from clio_relay.jobs.handles import JobHandle

    document = {"service_endpoints": [{"name": "pvserver", "port": 53173}]}
    seen: list[JobHandle] = []

    async def site_reads(self: LocalAdapter, handle: JobHandle) -> dict[str, Any]:
        seen.append(handle)
        return document

    monkeypatch.setattr(LocalAdapter, "service_runtimes", site_reads)
    client = TestClient(site_app(_settings(tmp_path / "site")))
    adapter = _adapter(tmp_path)

    async def call(path: str, body: dict[str, Any], *, timeout_s: float) -> Any:
        payload = {"namespace": "ab" * 8, **body}
        return client.post(f"/adapter{path}", headers=_headers(), json=payload).json()["ok"]

    monkeypatch.setattr(adapter, "call", call)
    remote = {"adapter": "local", "native_id": "29177", "data": {"mode": "jarvis"}}
    handle = JobHandle(adapter="worker", native_id="29177", data={"remote": remote})

    assert adapter.reads_service_runtimes
    assert asyncio.run(adapter.service_runtimes(handle)) == document
    assert seen[0].native_id == "29177"
    assert seen[0].data == {"mode": "jarvis"}


def test_a_serve_forward_from_a_worker_site_gets_a_plain_ssh_ending_at_the_host(
    tmp_path: Path,
) -> None:
    # serve puts -N -L before the last word; the sign-in's "bash -s" made bash read them.
    adapter = _adapter(tmp_path)

    assert adapter.ssh_argv() == ["ssh", "-o", "BatchMode=yes", "site-login"]
    assert adapter.sign_in_argv()[-3:] == ["site-login", "bash", "-s"]


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


def _placing(tmp_path: Path, **config: Any) -> tuple[WorkerAdapter, WorkerLink]:
    context = AdapterContext(
        site_id="site", state_dir=tmp_path / "adapter", content=ContentStore(tmp_path / "content")
    )
    token = tmp_path / "frps.token"
    token.write_text("frps-token", encoding="utf-8")
    rendezvous = Rendezvous(address="frps.example.test", port=7000, token_file=str(token))
    config.setdefault("relay_requirement", "clio-relay==0.0.0")
    adapter = WorkerAdapter(
        WorkerConfig(
            ssh_profile="site-login",
            rendezvous=rendezvous,
            **config,
        ),
        context,
    )
    link = WorkerLink(
        cluster="worker-site",
        session_id="session_1",
        generation_id="generation_1",
        api_token="api-token",
        session_owner_token="owner-token",
        stcp_secret="stcp-secret",
        proxy_name="clio-worker-0",
        port=8765,
        remote_dir="~/.local/share/clio-relay/worker/site",
    )
    return adapter, link


def test_placement_fetches_a_pinned_frpc_on_the_site(tmp_path: Path) -> None:
    """With no frpc set, the site fetches relay's pinned frpc and checks its sha256."""
    from clio_relay.adapters.worker.frpc_fetch import FRP_VERSION, PINNED

    adapter, link = _placing(tmp_path)

    script = adapter.placement_script(link, session_owner_token="owner-token")

    archive, digest = PINNED[("Linux", "x86_64|amd64")]
    assert f"v{FRP_VERSION}/{archive}" in script
    assert digest in script
    assert '"$got" != "$FRPC_SHA"' in script
    assert 'FRPC_PATH="$D/bin/frpc"' in script
    assert "CLIO_RELAY_FRPC" not in script


def test_placement_fetches_the_sites_own_frpc_url(tmp_path: Path) -> None:
    digest = "ab" * 32
    adapter, link = _placing(
        tmp_path, frpc_url="https://mirror.example.test/frpc.tgz", frpc_sha256=digest
    )

    script = adapter.placement_script(link, session_owner_token="owner-token")

    assert "FRPC_URL=https://mirror.example.test/frpc.tgz" in script
    assert f"FRPC_SHA={digest}" in script
    assert "uname -m" not in script


def test_placement_refuses_an_frpc_url_without_its_sha256(tmp_path: Path) -> None:
    adapter, link = _placing(tmp_path, frpc_url="https://mirror.example.test/frpc")

    with pytest.raises(Refusal) as refusal:
        adapter.placement_script(link, session_owner_token="owner-token")

    assert refusal.value.reason is RefusalReason.INVALID_CONFIG
    assert "frpc_sha256" in refusal.value.message


def test_placement_copies_in_the_frpc_built_for_the_site(tmp_path: Path) -> None:
    binary = tmp_path / "frpc-linux-amd64"
    binary.write_bytes(b"site frpc")
    adapter, link = _placing(tmp_path, frpc_binary=str(binary))

    script = adapter.placement_script(link, session_owner_token="owner-token")

    assert base64.b64encode(b"site frpc").decode("ascii") in script
    assert 'FRPC_PATH="$D/bin/frpc"' in script


def test_placement_uses_the_sites_own_frpc_when_set(tmp_path: Path) -> None:
    adapter, link = _placing(tmp_path, site_frpc="/opt/frp/frpc")

    script = adapter.placement_script(link, session_owner_token="owner-token")

    assert 'FRPC_PATH="/opt/frp/frpc"' in script
    assert "CLIO_RELAY_FRPC" not in script


def test_placement_with_no_relay_setting_sends_this_computers_relay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A site whose settings name no relay gets the wheel this computer's relay came from."""
    from clio_relay.adapters.worker import adapter as worker_adapter
    from clio_relay.adapters.worker.own_wheel import RelayInstall

    wheel = tmp_path / "clio_relay-9.9.9-py3-none-any.whl"
    wheel.write_bytes(b"relay wheel")
    asked: list[Path] = []

    def own(cache_dir: Path) -> RelayInstall:
        asked.append(cache_dir)
        return RelayInstall(wheel=wheel)

    monkeypatch.setattr(worker_adapter, "relay_install", own)
    adapter, link = _placing(tmp_path, relay_requirement=None)

    script = adapter.placement_script(link, session_owner_token="owner-token")

    assert asked == [tmp_path / "adapter" / "relay-wheel"]
    assert base64.b64encode(b"relay wheel").decode("ascii") in script
    assert '"$D/dist/clio_relay-9.9.9-py3-none-any.whl"' in script
    assert "clio-relay==" not in script


class _HeldWorker:
    connected = True

    def __init__(self) -> None:
        self.asked: list[str] = []
        self.closed = False

    def request_json(self, *, method: str, path: str, **_: Any) -> dict[str, Any]:
        self.asked.append(f"{method} {path}")
        return {"pid": 4242}

    def close(self) -> None:
        self.closed = True


def test_a_shutdown_the_worker_took_forgets_its_link(tmp_path: Path) -> None:
    """A second shutdown, as site clean runs after one, has no gone worker to wait on."""
    from clio_relay.adapters.worker import adapter as worker_adapter

    adapter, link = _placing(tmp_path)
    adapter._store_link(link)  # pyright: ignore[reportPrivateUsage]
    held = _HeldWorker()
    key = adapter._connection_key(link)  # pyright: ignore[reportPrivateUsage]
    with worker_adapter._connections_lock:  # pyright: ignore[reportPrivateUsage]
        worker_adapter._connections[key] = held  # pyright: ignore[reportPrivateUsage, reportArgumentType]

    stopped = asyncio.run(adapter.shutdown())

    assert held.asked == ["POST /shutdown"] and held.closed
    assert stopped[0]["kind"] == "site_serve" and stopped[0]["pid"] == 4242
    assert adapter.load_link() is None
    assert asyncio.run(adapter.shutdown()) == []

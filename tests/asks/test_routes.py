"""Relay's page under relay's own origin over Streamable HTTP, behind the operator credential."""

from __future__ import annotations

import base64

import pytest
from fastmcp import FastMCP
from starlette.testclient import TestClient

from clio_relay.asks.provider import read_base
from clio_relay.asks.routes import operator_token_file
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site, hidden_fields, provider_of

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

PUBLIC = "https://relay.example:8443"


def _basic(password: str) -> dict[str, str]:
    value = base64.b64encode(f"anyone:{password}".encode()).decode()
    return {"Authorization": f"Basic {value}"}


async def test_page_on_relays_origin(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_PUBLIC_URL", PUBLIC)
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths, transport="http")
    provider = provider_of(service)
    server: FastMCP[None] = FastMCP("relay-test")
    provider.mount(server)
    assert read_base(relay_paths.state_dir) == PUBLIC
    record = await service.start_run("fx", JobSpec(executable="echo", arguments=["hi"]))
    (ask,) = service.outstanding(record)
    url = provider.page_address(ask)
    assert url is not None and url.startswith(f"{PUBLIC}/relay/asks/{ask.ask_id}?t=")
    path = url.removeprefix(PUBLIC)
    token = operator_token_file(relay_paths.state_dir).read_text(encoding="ascii").strip()
    assert (operator_token_file(relay_paths.state_dir).stat().st_mode & 0o077) == 0
    with TestClient(server.http_app(), base_url=PUBLIC) as client:
        refused = client.get(path)
        assert refused.status_code == 401
        assert "WWW-Authenticate" in refused.headers
        assert client.get(path, headers=_basic("wrong")).status_code == 401
        page = client.get(path, headers=_basic(token))
        assert page.status_code == 200
        assert page.headers["X-Frame-Options"] == "DENY"
        form = hidden_fields(page.text) | {"action": "accept"}
        target = path.split("?", 1)[0]
        # The page's tokens without the operator credential approve nothing.
        assert client.post(target, data=form, headers={"Origin": PUBLIC}).status_code == 401
        foreign = client.post(
            target, data=form, headers={"Origin": "https://evil.example", **_basic(token)}
        )
        assert foreign.status_code == 403
        assert service.asks.get(ask.ask_id) is not None
        assert service.asks.get(ask.ask_id).state == "pending"  # type: ignore[union-attr]
        posted = client.post(target, data=form, headers={"Origin": PUBLIC, **_basic(token)})
        assert posted.status_code == 200, posted.text
        assert client.get(path, headers=_basic(token)).status_code == 404
    advanced = await service.advance(record.operation_id)
    assert advanced.state == OperationState.QUEUED
    await service.close()


async def test_no_public_address_keeps_the_page_on_loopback(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths, transport="http")
    provider = provider_of(service)
    server: FastMCP[None] = FastMCP("relay-test")
    provider.mount(server)
    record = await service.start_run("fx", JobSpec(executable="echo"))
    (ask,) = service.outstanding(record)
    url = provider.page_address(ask)
    assert url is not None and url.startswith("http://127.0.0.1:")
    await service.close()

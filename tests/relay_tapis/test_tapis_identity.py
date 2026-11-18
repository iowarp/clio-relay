"""LQ39 and the TACC identity: device code sign-in, refresh, refusal and sign-out.

The person signs in on TACC's https page through one URL elicitation. Nothing
secret (the device code, the tokens, the OAuth client's key) reaches the model,
the ask records, relay's state files or the log.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Any, cast

import mcp_types
import pytest
from fastmcp import Client
from fastmcp.client.elicitation import ElicitResult

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.tapis import TapisAdapter, TapisConfig
from clio_relay.identity.tacc import MemoryStore, TaccIdentity, TaccSettings
from clio_relay.jobs.content import ContentStore
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.app import VERIFICATION_BASE
from tests.fixtures.tapis_stub.support import (
    CLIENT_ID,
    CLIENT_KEY,
    USERNAME,
    identity,
    seed_host,
    sign_in,
    tapis_config,
    use_memory_keyring,
)

from .conftest import add_site

pytestmark = pytest.mark.release_platform("posix")

SITE = "tp"
ECHO: dict[str, Any] = {"executable": "echo", "arguments": ["hello-tacc"]}
CODE = re.compile(r"enter the code ([A-Z0-9-]+)")


def _site(stub: TapisStub) -> dict[str, Any]:
    return {
        "id": SITE,
        "adapter": "tapis",
        "identity": "tacc",
        "config": tapis_config(stub),
        "policy": "open",
        "spend": {"ask": "never"},
    }


def _structured(result: Any) -> dict[str, Any]:
    structured = result.structured_content
    assert isinstance(structured, dict), result
    return structured  # pyright: ignore[reportUnknownVariableType]


def _secrets(stub: TapisStub, store: MemoryStore) -> set[str]:
    found = {CLIENT_KEY}
    for item in stub.state.requests_to("/v3/oauth2/tokens", "POST"):
        body = json.loads(item["body"] or "{}")
        found.update(str(body[key]) for key in ("device_code", "refresh_token") if body.get(key))
    for raw in store.values.values():
        record = json.loads(raw)
        found.update(
            str(record[key]) for key in ("access_token", "refresh_token") if record.get(key)
        )
        pending = cast(dict[str, Any], record.get("pending") or {})
        if pending.get("device_code"):
            found.add(str(pending["device_code"]))
    return {item for item in found if item}


def _relay_files(paths: RelayPaths) -> str:
    texts: list[str] = []
    for root in (paths.config_dir, paths.state_dir):
        for path in root.rglob("*"):
            if path.is_file():
                texts.append(path.read_bytes().decode("utf-8", errors="replace"))
    return "\n".join(texts)


def test_sign_in_is_one_url_elicitation_and_no_secret_leaks(
    stub: TapisStub,
    relay_paths: RelayPaths,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """LQ39: an MCP client signs in through one https URL elicitation."""
    caplog.set_level(logging.DEBUG)
    store = use_memory_keyring(monkeypatch)
    seed_host(stub, SITE, tmp_path / "site")
    add_site(relay_paths, _site(stub))
    elicited: list[dict[str, Any]] = []

    async def person(message: str, response_type: Any, params: Any, context: Any) -> Any:
        dumped = params.model_dump(mode="json", by_alias=True)
        elicited.append({"message": message, "params": dumped})
        assert isinstance(params, mcp_types.ElicitRequestURLParams), dumped
        code = CODE.search(message)
        assert code is not None, message
        # The person opens the TACC page, types the code and signs in.
        assert stub.state.approve(code.group(1), USERNAME)
        return ElicitResult(action="accept", content=None)

    transcript: list[str] = []

    async def scenario() -> None:
        async with Client(
            create_server(relay_paths), mode="legacy", elicitation_handler=person
        ) as client:
            result = await client.call_tool("run", {"site": SITE, "spec": ECHO, "wait_s": 300})
            transcript.append(json.dumps(result.structured_content, default=str))
            transcript.extend(str(item) for item in result.content)
            view = _structured(result)
            assert view["state"] == "completed", view
            assert "hello-tacc" in view["result"]["stdout_tail"]
            described = await client.call_tool("describe", {"site": SITE})
            transcript.append(json.dumps(described.structured_content, default=str))
            facts = _structured(described)["info"]["facts"]
            assert facts["signed_in"] is True and facts["account"] == USERNAME

    asyncio.run(scenario())
    (only,) = elicited
    assert only["params"]["mode"] == "url"
    assert only["params"]["url"].startswith("https://")
    assert only["params"]["url"] == f"{VERIFICATION_BASE}/v3/oauth2/device?client_id={CLIENT_ID}"
    transcript.extend(json.dumps(item) for item in elicited)
    secrets = _secrets(stub, store)
    assert len(secrets) >= 4, "the device code, both tokens and the client key"
    asks = RelayService(relay_paths).asks.all()
    places = {
        "transcript": "\n".join(transcript),
        "asks": json.dumps([ask.model_dump(mode="json") for ask in asks]),
        "relay files": _relay_files(relay_paths),
        "log": caplog.text,
    }
    for where, text in places.items():
        for secret in secrets:
            assert secret not in text, f"a secret reached the {where}"
    print(f"LQ39 elicitation {only}")
    print(f"LQ39 checked {len(secrets)} secrets in {sorted(places)}")


def test_client_without_elicitation_gets_needs_sign_in_with_the_address(
    stub: TapisStub, relay_paths: RelayPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LQ39: a client with no elicitation gets the typed refusal carrying TACC's address."""
    use_memory_keyring(monkeypatch)
    seed_host(stub, SITE, tmp_path / "site")
    add_site(relay_paths, _site(stub))

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            result = await client.call_tool(
                "run", {"site": SITE, "spec": ECHO, "wait_s": 0}, raise_on_error=False
            )
            assert result.is_error
            refusal = _structured(result)["refusal"]
            print(f"LQ39 refusal {refusal}")
            assert refusal["reason"] == "needs_sign_in"
            (ask,) = refusal["data"]["asks"]
            assert ask["page"].startswith("https://")
            code = CODE.search(ask["message"])
            assert code is not None
            assert stub.state.approve(code.group(1), USERNAME)
            # The refusal's own route: the person signs in at TACC, then answers the ask.
            RelayService(relay_paths).answer(ask["ask_id"], "accept", by="person")
            resumed = await client.call_tool(
                "run", {"resume": refusal["data"]["operation_id"], "wait_s": 300}
            )
            assert _structured(resumed)["state"] == "completed"

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_poll_waits_for_the_interval_unless_forced(
    stub: TapisStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_memory_keyring(monkeypatch)
    now = [1_000_000.0]
    found = TaccIdentity(
        settings=TaccSettings.resolve(None, base_url=stub.base_url), clock=lambda: now[0]
    )
    step = await found.begin()
    assert step.url.startswith("https://") and not step.relay_page
    assert (await found.begin()).display == step.display, "an open code is reused"
    before = len(stub.state.requests_to("/v3/oauth2/tokens"))
    assert (await found.poll()).state == "pending"
    assert len(stub.state.requests_to("/v3/oauth2/tokens")) == before
    now[0] += step.poll_interval_s or 5.0
    assert (await found.poll()).state == "pending"
    assert len(stub.state.requests_to("/v3/oauth2/tokens")) == before + 1
    assert (await found.poll()).state == "pending"
    assert len(stub.state.requests_to("/v3/oauth2/tokens")) == before + 1
    assert stub.state.approve(step.display["user_code"], USERNAME)
    assert (await found.poll(force=True)).state == "done"
    status = await found.status()
    assert status.state == "signed_in" and status.account == USERNAME


@pytest.mark.asyncio
async def test_an_expired_code_fails_and_a_new_one_is_issued(
    stub: TapisStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_memory_keyring(monkeypatch)
    now = [1_000_000.0]
    found = TaccIdentity(
        settings=TaccSettings.resolve(None, base_url=stub.base_url), clock=lambda: now[0]
    )
    first = await found.begin()
    now[0] += 601
    assert found.pending() is None
    assert (await found.poll(force=True)).state == "failed"
    second = await found.begin()
    assert second.display["user_code"] != first.display["user_code"]


@pytest.mark.asyncio
async def test_refresh_replaces_the_access_token(
    stub: TapisStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_memory_keyring(monkeypatch)
    monkeypatch.setattr(stub.state, "access_ttl_s", 60)
    found = await sign_in(stub)
    first = await found.access_token()
    assert first is not None
    refreshed = [
        item
        for item in stub.state.requests_to("/v3/oauth2/tokens", "POST")
        if json.loads(item["body"]).get("grant_type") == "refresh_token"
    ]
    second = await found.access_token()
    after = [
        item
        for item in stub.state.requests_to("/v3/oauth2/tokens", "POST")
        if json.loads(item["body"]).get("grant_type") == "refresh_token"
    ]
    assert second is not None and second != first
    assert len(after) == len(refreshed) + 1
    assert (await found.status()).state == "signed_in"


@pytest.mark.asyncio
async def test_lifetimes_and_a_forced_refresh_show_no_token(
    stub: TapisStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_memory_keyring(monkeypatch)
    found = await sign_in(stub)
    first = await found.access_token()
    assert first is not None
    times = found.lifetimes()
    assert times["account"] == USERNAME and times["can_refresh"]
    assert times["access"]["remaining_s"] > 29 * 86400
    assert times["refresh"]["expires_at"] is not None
    assert await found.refresh_now()
    second = await found.access_token()
    assert second is not None and second != first
    shown = json.dumps(found.lifetimes())
    store = cast(MemoryStore, found.store)
    for secret in _secrets(stub, store) | {first, second}:
        assert secret not in shown


@pytest.mark.asyncio
async def test_without_the_client_key_there_is_no_refresh(
    stub: TapisStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_memory_keyring(monkeypatch)
    monkeypatch.setattr(stub.state, "access_ttl_s", 1)
    await sign_in(stub)
    monkeypatch.delenv("CLIO_RELAY_TACC_CLIENT_KEY")
    now = [2_000_000_000.0]
    keyless = TaccIdentity(
        settings=TaccSettings.resolve(None, base_url=stub.base_url), clock=lambda: now[0]
    )
    assert keyless.settings.client_key is None
    assert await keyless.token() is None
    assert (await keyless.status()).state == "expired"


@pytest.mark.asyncio
async def test_a_refused_refresh_signs_out_and_asks_again(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_memory_keyring(monkeypatch)
    monkeypatch.setattr(stub.state, "access_ttl_s", 60)
    await sign_in(stub)
    monkeypatch.setattr(stub.state, "refused_refresh", True)
    context = AdapterContext(
        site_id=SITE, state_dir=tmp_path / "relay", content=ContentStore(tmp_path / "content")
    )
    adapter = TapisAdapter(TapisConfig.model_validate(tapis_config(stub)), context)
    need = await adapter.sign_in_needed()
    assert need is not None
    assert need.url is not None and need.url.startswith("https://")
    assert (await adapter.identity.status()).state == "signed_out"
    assert adapter.identity.pending() is not None


@pytest.mark.asyncio
async def test_sign_out_revokes_and_forgets(
    stub: TapisStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = use_memory_keyring(monkeypatch)
    found = await sign_in(stub)
    access = await found.access_token()
    assert access is not None and store.values
    assert access not in repr(await found.token())
    await found.sign_out()
    assert access in stub.state.revoked
    assert not store.values
    assert (await identity(stub).status()).state == "signed_out"

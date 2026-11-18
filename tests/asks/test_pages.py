"""Relay's page: what GET shows, what POST accepts, and what it refuses (RL25a, RL28)."""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pytest

from clio_relay.adapters.base import SignInNeed
from clio_relay.adapters.hooks import Ask
from clio_relay.asks.core import origin_ok
from clio_relay.asks.pages import SECURITY_HEADERS, binding_table
from clio_relay.asks.provider import base_file, read_base
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site, hidden_fields, origin_of, provider_of, submit

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

ECHO = JobSpec(executable="echo", arguments=["hello-relay"])


async def _approval(paths: RelayPaths) -> tuple[RelayService, Ask, str]:
    add_site(paths, spend="always")
    service = RelayService(paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    url = provider_of(service).page_address(ask)
    assert url is not None
    return service, ask, url


def _state(service: RelayService, ask_id: str) -> Ask:
    found = service.asks.get(ask_id)
    assert found is not None
    return found


async def test_get_shows_the_binding_and_changes_nothing(relay_paths: RelayPaths) -> None:
    service, ask, url = await _approval(relay_paths)
    assert url.startswith("http://127.0.0.1:")
    assert read_base(relay_paths.state_dir) == origin_of(url)
    assert base_file(relay_paths.state_dir).is_file()
    pages = [httpx.get(url, timeout=30) for _ in range(3)]
    assert [item.status_code for item in pages] == [200, 200, 200]
    page = pages[-1]
    for key, value in SECURITY_HEADERS.items():
        assert page.headers[key] == value
    assert ask.binding["spec_digest"] in page.text
    assert ask.operation_id is not None and ask.operation_id in page.text
    assert "Approve" in page.text and "Decline" in page.text
    assert _state(service, ask.ask_id).state == "pending"
    await service.close()


async def test_approve_on_the_page_once(relay_paths: RelayPaths) -> None:
    service, ask, url = await _approval(relay_paths)
    posted = submit(url, "accept")
    assert posted.status_code == 200, posted.text
    stored = _state(service, ask.ask_id)
    assert stored.state == "accepted"
    assert stored.answered_by == "page"
    assert stored.answer == {"action": "accept", "content": {"approve": True}}
    assert ask.operation_id is not None
    advanced = await service.advance(ask.operation_id)
    assert advanced.state == OperationState.QUEUED
    # The address dies with the answer, so a replayed form does nothing.
    assert httpx.get(url, timeout=30).status_code == 404
    replay = httpx.post(
        url.split("?", 1)[0],
        data={"t": url.split("t=", 1)[1], "csrf": "x", "action": "decline"},
        timeout=30,
    )
    assert replay.status_code == 404
    assert _state(service, ask.ask_id).state == "accepted"
    done = await service.wait(ask.operation_id, 60, poll_s=0.1)
    assert done.state == OperationState.COMPLETED
    await service.close()


async def test_post_needs_the_form_token(relay_paths: RelayPaths) -> None:
    service, ask, url = await _approval(relay_paths)
    target = url.split("?", 1)[0]
    token = url.split("t=", 1)[1]
    origin = {"Origin": origin_of(url)}
    for form in (
        {"t": token, "action": "accept"},
        {"t": token, "csrf": "0" * 64, "action": "accept"},
        {"t": "wrong", "csrf": "0" * 64, "action": "accept"},
    ):
        assert httpx.post(target, data=form, headers=origin, timeout=30).status_code == 404
    assert _state(service, ask.ask_id).state == "pending"
    await service.close()


@pytest.mark.parametrize(
    "headers",
    [
        {"Origin": "http://evil.example"},
        {"Origin": "null"},
        {"Sec-Fetch-Site": "cross-site"},
        {"Origin": "", "Referer": "http://evil.example/page"},
    ],
)
async def test_cross_site_posts_are_refused(
    relay_paths: RelayPaths, headers: dict[str, str]
) -> None:
    service, ask, url = await _approval(relay_paths)
    posted = submit(url, "accept", headers=headers)
    assert posted.status_code == 403
    assert _state(service, ask.ask_id).state == "pending"
    await service.close()


async def test_origin_rule() -> None:
    allowed = {"http://127.0.0.1:5000"}
    assert origin_ok({"origin": "http://127.0.0.1:5000"}, allowed)
    assert origin_ok({"referer": "http://127.0.0.1:5000/relay/asks/x"}, allowed)
    assert origin_ok({"sec-fetch-site": "same-origin"}, allowed)
    # No browser headers at all: a script holding the page's tokens, not a forged form.
    assert origin_ok({}, allowed)
    assert not origin_ok({"origin": "http://127.0.0.1:5001"}, allowed)
    assert not origin_ok({"origin": "null"}, allowed)
    assert not origin_ok({"sec-fetch-site": "same-site"}, allowed)


async def test_expired_address_keeps_the_ask(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    provider = provider_of(service)
    provider.tokens.ttl_s = -1.0
    old = provider.page_address(ask)
    assert old is not None
    page = httpx.get(old, timeout=30)
    assert page.status_code == 410
    assert f"clio-relay asks page {ask.ask_id}" in page.text
    provider.tokens.ttl_s = 3600.0
    fresh = provider.page_address(ask)
    assert fresh is not None and fresh != old
    assert submit(fresh, "accept").status_code == 200
    assert _state(service, ask.ask_id).state == "accepted"
    await service.close()


@pytest.mark.parametrize("field", ["op", "bd"])
async def test_a_form_for_something_else_is_refused(relay_paths: RelayPaths, field: str) -> None:
    service, ask, url = await _approval(relay_paths)
    posted = submit(url, "accept", extra={field: "something-else"})
    assert posted.status_code == 409
    assert _state(service, ask.ask_id).state == "pending"
    await service.close()


@pytest.mark.parametrize("change", ["spec_digest", "charge", "state"])
async def test_approval_is_bound_to_the_job_and_charge(
    relay_paths: RelayPaths, change: str
) -> None:
    service, ask, url = await _approval(relay_paths)
    assert ask.operation_id is not None
    page = httpx.get(url, timeout=30)
    with service.operations.locked(ask.operation_id):
        record = service.operation(ask.operation_id)
        if change == "spec_digest":
            record.spec_digest = "0" * 64
        elif change == "charge":
            charge: dict[str, Any] = dict((record.result or {}).get("charge") or {})
            charge["node_hours"] = 9999
            record.result = {**(record.result or {}), "charge": charge}
        else:
            record.move(OperationState.CANCELLED)
        service.operations.put(ask.operation_id, record)
    form = hidden_fields(page.text) | {"action": "accept"}
    posted = httpx.post(
        url.split("?", 1)[0], data=form, headers={"Origin": origin_of(url)}, timeout=30
    )
    assert posted.status_code == 409
    assert "Nothing was approved" in posted.text
    assert _state(service, ask.ask_id).state == "pending"
    await service.close()


async def test_tampered_ask_binding_is_refused(relay_paths: RelayPaths) -> None:
    service, ask, url = await _approval(relay_paths)
    page = httpx.get(url, timeout=30)
    with service.asks.locked(ask.ask_id):
        stored = _state(service, ask.ask_id)
        stored.binding = {**stored.binding, "site": "elsewhere"}
        service.asks.put(ask.ask_id, stored)
    form = hidden_fields(page.text) | {"action": "accept"}
    form.pop("bd")
    posted = httpx.post(
        url.split("?", 1)[0], data=form, headers={"Origin": origin_of(url)}, timeout=30
    )
    assert posted.status_code == 409
    assert _state(service, ask.ask_id).state == "pending"
    await service.close()


async def test_decline_on_the_page_cancels(relay_paths: RelayPaths) -> None:
    service, ask, url = await _approval(relay_paths)
    assert submit(url, "decline").status_code == 200
    assert _state(service, ask.ask_id).state == "declined"
    assert ask.operation_id is not None
    final = await service.advance(ask.operation_id)
    assert final.state == OperationState.CANCELLED
    assert final.handle is None
    await service.close()


async def test_choose_on_the_page(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    provider = provider_of(service)
    ask = await provider.choose(
        site=service.site("fx"),
        operation_id=None,
        message="Which queue?",
        field_name="queue",
        options=["debug", "batch"],
    )
    url = provider.page_address(ask)
    assert url is not None
    page = httpx.get(url, timeout=30)
    assert "value='debug'" in page.text and "value='batch'" in page.text
    assert submit(url, "accept", "gpu").status_code == 400
    assert _state(service, ask.ask_id).state == "pending"
    assert submit(url, "accept", "batch").status_code == 200
    stored = _state(service, ask.ask_id)
    assert stored.answer == {"action": "accept", "content": {"queue": "batch"}}
    await service.close()


async def test_an_external_sign_in_points_at_the_site(relay_paths: RelayPaths) -> None:
    site = add_site(relay_paths)
    service = RelayService(relay_paths)
    provider = provider_of(service)
    need = SignInNeed(provider="site-sso", message="Sign in", url="https://sso.example/login")
    ask = await provider.url(site=site, operation_id=None, need=need)
    assert ask.url == "https://sso.example/login"
    assert ask.binding["mode"] == "external"
    assert provider.page_address(ask) == "https://sso.example/login"
    await service.close()


async def test_a_typed_secret_goes_to_the_keyring_only(
    relay_paths: RelayPaths, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    site = add_site(relay_paths)
    service = RelayService(relay_paths)
    provider = provider_of(service)
    need = SignInNeed(provider="keyring:cloud-key", message="Relay needs the cloud key")
    ask = await provider.url(site=site, operation_id=None, need=need)
    assert ask.binding["mode"] == "keyring"
    assert ask.url is not None
    page = httpx.get(ask.url, timeout=30)
    assert "type='password'" in page.text
    secret = "s3cret-value-for-the-test"
    posted = submit(ask.url, "secret", secret)
    assert posted.status_code == 200
    assert provider.token_store().secret("cloud-key") == secret
    assert _state(service, ask.ask_id).completed_at is not None
    assert secret not in caplog.text
    for path in relay_paths.state_dir.rglob("*"):
        if path.is_file():
            assert secret.encode() not in path.read_bytes(), path
    await service.close()


async def test_a_bad_ask_id_is_not_found(relay_paths: RelayPaths) -> None:
    service, _, url = await _approval(relay_paths)
    base = origin_of(url)
    assert httpx.get(f"{base}/relay/asks/..%2Fetc?t=x", timeout=30).status_code == 404
    assert httpx.get(f"{base}/elsewhere", timeout=30).status_code == 404
    await service.close()

def test_a_package_review_shows_the_package() -> None:
    ask = Ask(ask_id="ask_pkg", kind="approve", site="fx", operation_id="op_pkg", message="Review")
    ask.bind(
        {
            "kind": "package_review",
            "site": "fx",
            "package": "numpy<b>",
            "version": "2.1.0",
            "source": "pypi",
            "package_hash": "sha256:abc",
        }
    )
    table = binding_table(ask)
    for row in ("<th>Package</th><td>numpy&lt;b&gt;</td>", "<th>Version</th><td>2.1.0</td>"):
        assert row in table
    assert "<th>Source</th><td>pypi</td>" in table
    assert "<th>Package hash</th><td>sha256:abc</td>" in table
    plain = Ask(ask_id="ask_job", kind="approve", site="fx", message="Run")
    plain.bind({"site": "fx", "spec_digest": "d"})
    assert "Package" not in binding_table(plain) and "Source" not in binding_table(plain)
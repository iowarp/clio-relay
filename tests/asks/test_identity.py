"""Identities: the provider table, grant files, the keyring, and the Globus and GitHub sign-ins.

Globus and GitHub run against stubs. The live Globus sign-in waits on a registered
native-app client (owner blocker O10).
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from clio_relay import identity
from clio_relay.adapters.base import SignInNeed
from clio_relay.adapters.hooks import Ask
from clio_relay.asks import signin
from clio_relay.asks.provider import RelayAskProvider
from clio_relay.identity import (
    GrantFiles,
    Identities,
    IdentityContext,
    IdentityStatus,
    SignInContext,
    SignInResult,
    SignInStep,
    Token,
    TokenStore,
)
from clio_relay.identity import probe as probe_module
from clio_relay.identity.github import GitHubIdentity
from clio_relay.identity.globus import GlobusIdentity
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site, eventually, hidden_fields, provider_of, submit

pytestmark = [pytest.mark.release_platform("posix")]

AUTH = "https://auth.stub.invalid"
GITHUB = "https://github.stub.invalid"


def write_grant(path: Path, provider: str, token: str, *, hours: float = 1.0) -> None:
    expires = (utc_now() + timedelta(hours=hours)).isoformat()
    document = {
        "version": 1,
        "provider": provider,
        "written_at": utc_now().isoformat(),
        "tokens": [
            {
                "resource_server": "transfer.api.globus.org",
                "access_token": token,
                "expires_at": expires,
                "scopes": ["urn:globus:auth:scope:transfer.api.globus.org:all"],
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(document), encoding="utf-8")
    tmp.replace(path)


def _context(grants: GrantFiles | None = None) -> IdentityContext:
    return IdentityContext(store=TokenStore("memory"), grants=grants or GrantFiles())


# The table


class _Stub:
    name = "stubid"
    title = "Stub"

    def __init__(self, context: IdentityContext) -> None:
        self.context = context

    async def status(self) -> IdentityStatus:
        return IdentityStatus(provider=self.name, title=self.title, state="signed_out")

    async def token(self, resource_server: str | None = None) -> Token | None:
        return None

    async def begin(self, ctx: SignInContext) -> SignInStep:
        return SignInStep(url="https://stub.example/")

    async def poll(self, ctx: SignInContext, step: SignInStep) -> SignInResult:
        return SignInResult(state="pending")

    async def callback(self, ctx: SignInContext, params: dict[str, str]) -> SignInResult:
        return SignInResult(state="failed")

    async def sign_out(self) -> None:
        return None


def test_the_table_ships_globus_and_github() -> None:
    names = identity.names()
    assert "globus" in names and "github" in names
    assert identity.get("GLOBUS") is not None
    assert identity.get("nothing-like-this") is None
    built = Identities(_context()).get("globus")
    assert isinstance(built, GlobusIdentity)
    assert isinstance(built, identity.IdentityProvider)


def test_register_adds_and_removes_a_provider() -> None:
    identity.register("stubid", _Stub)
    try:
        assert "stubid" in identity.names()
        built = Identities(_context()).get("StubID")
        assert isinstance(built, _Stub)
        assert isinstance(built, identity.IdentityProvider)
    finally:
        identity.register("stubid", None)
    assert identity.get("stubid") is None


# Grant files (HS4a)


def test_grant_file_is_read_again_when_it_changes(tmp_path: Path) -> None:
    path = tmp_path / "grants" / "globus.json"
    grants = GrantFiles({"globus": path})
    assert grants.read("globus") is None
    assert grants.problem("globus") is None
    write_grant(path, "globus", "first-token")
    found = grants.token("globus")
    assert found is not None and found.access_token.get_secret_value() == "first-token"
    assert found.source == "grant"
    assert grants.changed() == []
    write_grant(path, "globus", "second-token-longer")
    assert grants.changed() == ["globus"]
    found = grants.token("globus", "transfer.api.globus.org")
    assert found is not None and found.access_token.get_secret_value() == "second-token-longer"
    assert "second-token-longer" not in repr(found)
    path.unlink()
    assert grants.token("globus") is None


@pytest.mark.parametrize(
    ("document", "problem"),
    [
        ("{not json", "unreadable grant file"),
        (json.dumps({"version": 2, "provider": "globus"}), "version 2"),
        (json.dumps({"version": 1, "provider": "github"}), "not 'globus'"),
    ],
)
def test_a_bad_grant_file_says_why(tmp_path: Path, document: str, problem: str) -> None:
    path = tmp_path / "globus.json"
    path.write_text(document, encoding="utf-8")
    grants = GrantFiles({"globus": path})
    assert grants.read("globus") is None
    found = grants.problem("globus")
    assert found is not None and problem in found
    assert "not json" not in found


def test_an_expired_grant_token_is_not_used(tmp_path: Path) -> None:
    path = tmp_path / "globus.json"
    write_grant(path, "globus", "old", hours=-1)
    assert GrantFiles({"globus": path}).token("globus") is None


def test_grant_files_from_settings_and_env(tmp_path: Path) -> None:
    grants = GrantFiles.from_settings(
        {"grant_files": {"Globus": str(tmp_path / "a.json")}},
        env={"CLIO_RELAY_GRANT_FILE_GITHUB": str(tmp_path / "b.json")},
    )
    assert grants.providers() == ["github", "globus"]


@pytest.mark.asyncio
async def test_status_prefers_the_grant(tmp_path: Path) -> None:
    path = tmp_path / "globus.json"
    context = _context(GrantFiles({"globus": path}))
    globus = GlobusIdentity(context)
    assert (await globus.status()).state == "signed_out"
    write_grant(path, "globus", "granted")
    status = await globus.status()
    assert status.state == "signed_in" and status.source == "grant"
    token = await globus.token()
    assert token is not None and token.access_token.get_secret_value() == "granted"
    await globus.sign_out()
    assert path.is_file()


# The keyring


def test_tokens_live_in_the_os_keyring(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    keyring = pytest.importorskip("keyring")
    plaintext = pytest.importorskip("keyrings.alt.file")
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("CLIO_RELAY_KEYRING")
    before = keyring.get_keyring()
    keyring.set_keyring(plaintext.PlaintextKeyring())
    try:
        store = TokenStore()
        assert store.kind == "keyring"
        store.set("identity:globus", {"tokens": [{"access_token": "kept"}]})
        assert TokenStore().get("identity:globus") == {"tokens": [{"access_token": "kept"}]}
        assert keyring.get_password("clio-relay", "identity:globus") is not None
        store.set_secret("cloud", "typed")
        assert TokenStore().secret("cloud") == "typed"
        store.delete("identity:globus")
        store.delete("identity:globus")
        assert store.get("identity:globus") is None
    finally:
        keyring.set_keyring(before)


# Globus, against a stub of Globus Auth


class GlobusStub:
    """Globus Auth's token endpoint, checking PKCE the way Globus does."""

    def __init__(self) -> None:
        self.challenges: dict[str, str] = {}
        self.redirects: list[str] = []
        self.issued = 0

    def remember(self, authorize_url: str) -> str:
        query = parse_qs(urlsplit(authorize_url).query)
        assert query["code_challenge_method"] == ["S256"]
        assert query["client_id"] == ["stub-native-client"]
        self.challenges["the-code"] = query["code_challenge"][0]
        return query["state"][0]

    def handler(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/oauth2/token"
        form = parse_qs(request.content.decode())
        assert "client_secret" not in form
        if form["grant_type"] == ["refresh_token"]:
            return httpx.Response(400, json={"error": "invalid_grant"})
        verifier = form["code_verifier"][0]
        digest = hashlib.sha256(verifier.encode()).digest()
        challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
        code = form["code"][0]
        if self.challenges.get(code) != challenge:
            return httpx.Response(400, json={"error": "invalid_grant"})
        self.redirects.append(form["redirect_uri"][0])
        self.issued += 1
        claims = base64.urlsafe_b64encode(json.dumps({"preferred_username": "p@x.org"}).encode())
        return httpx.Response(
            200,
            json={
                "access_token": "auth-access",
                "expires_in": 3600,
                "resource_server": "auth.globus.org",
                "scope": "openid profile email",
                "refresh_token": "auth-refresh",
                "id_token": f"e30.{claims.decode().rstrip('=')}.sig",
                "other_tokens": [
                    {
                        "access_token": "transfer-access",
                        "expires_in": 3600,
                        "resource_server": "transfer.api.globus.org",
                        "scope": "urn:globus:auth:scope:transfer.api.globus.org:all",
                        "refresh_token": "transfer-refresh",
                    }
                ],
            },
        )


def _stubbed(provider: RelayAskProvider, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    async def make() -> None:
        provider._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))  # pyright: ignore[reportPrivateUsage]

    provider.runner.submit(make()).result()


async def _identity_ask(service: RelayService, name: str) -> Ask:
    provider = provider_of(service)
    need = SignInNeed(provider=name, message=f"Sign in to {name}")
    ask = await provider.url(site=service.site("fx"), operation_id=None, need=need)
    assert ask.binding["mode"] == "identity"
    return ask


def _completed(service: RelayService, ask_id: str) -> Ask | None:
    found = service.asks.get(ask_id)
    return found if found is not None and found.completed_at is not None else None


@pytest.mark.asyncio
async def test_globus_redirect_sign_in(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_GLOBUS_CLIENT_ID", "stub-native-client")
    monkeypatch.setenv("CLIO_RELAY_GLOBUS_AUTH_URL", AUTH)
    add_site(relay_paths, identity="globus")
    service = RelayService(relay_paths)
    provider = provider_of(service)
    stub = GlobusStub()
    _stubbed(provider, stub.handler)
    ask = await _identity_ask(service, "globus")
    assert ask.url is not None
    with httpx.Client(timeout=30) as client:
        page = client.get(ask.url)
        assert "Sign in with Globus" in page.text
        assert submit(ask.url, "start", client=client).status_code == 303
        page = client.get(ask.url)
        link = page.text.split("href='", 1)[1].split("'", 1)[0].replace("&amp;", "&")
        assert link.startswith(f"{AUTH}/v2/oauth2/authorize?")
        state = stub.remember(link)
        callback = parse_qs(urlsplit(link).query)["redirect_uri"][0]
        assert callback == f"{provider.base_url()}/relay/asks/callback"
        assert client.get(callback, params={"code": "the-code", "state": "other"}).status_code == 404
        done = client.get(callback, params={"code": "the-code", "state": state})
        assert done.status_code == 200, done.text
        assert "Signed in" in done.text
        assert client.get(callback, params={"code": "the-code", "state": state}).status_code == 404
    assert stub.redirects == [callback]
    assert _completed(service, ask.ask_id) is not None
    globus = provider.identities().get("globus")
    assert globus is not None
    status = await globus.status()
    assert status.state == "signed_in" and status.source == "keyring"
    assert status.account == "p@x.org"
    transfer = await globus.token("transfer.api.globus.org")
    assert transfer is not None and transfer.access_token.get_secret_value() == "transfer-access"
    probes = eventually(
        lambda: probe_module.load(provider.paths()).get("fx"), "the probe after sign-in"
    )
    assert probes["state"] == "ready" and probes["identity"] == "globus"
    await service.close()


@pytest.mark.asyncio
async def test_globus_paste_sign_in(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_GLOBUS_CLIENT_ID", "stub-native-client")
    monkeypatch.setenv("CLIO_RELAY_GLOBUS_AUTH_URL", AUTH)
    monkeypatch.setenv("CLIO_RELAY_GLOBUS_REDIRECT", "paste")
    add_site(relay_paths)
    service = RelayService(relay_paths)
    provider = provider_of(service)
    stub = GlobusStub()
    _stubbed(provider, stub.handler)
    ask = await _identity_ask(service, "globus")
    assert ask.url is not None
    assert submit(ask.url, "start").status_code == 303
    page = httpx.get(ask.url, timeout=30)
    assert "name='value'" in page.text and "Finish sign-in" in page.text
    link = page.text.split("href='", 1)[1].split("'", 1)[0].replace("&amp;", "&")
    stub.remember(link)
    assert parse_qs(urlsplit(link).query)["redirect_uri"] == [f"{AUTH}/v2/web/auth-code"]
    wrong = submit(ask.url, "code", "not-the-code")
    assert wrong.status_code == 303
    assert "Globus refused the code" in httpx.get(ask.url, timeout=30).text
    assert _completed(service, ask.ask_id) is None
    # A failed sign-in starts again from the page.
    assert submit(ask.url, "start").status_code == 303
    page = httpx.get(ask.url, timeout=30)
    link = page.text.split("href='", 1)[1].split("'", 1)[0].replace("&amp;", "&")
    stub.remember(link)
    done = submit(ask.url, "code", "the-code")
    assert done.status_code == 200 and "Signed in" in done.text
    assert stub.redirects == [f"{AUTH}/v2/web/auth-code"]
    assert _completed(service, ask.ask_id) is not None
    await service.close()


@pytest.mark.asyncio
async def test_globus_without_a_client_says_so(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    ask = await _identity_ask(service, "globus")
    assert ask.url is not None
    assert submit(ask.url, "start").status_code == 303
    assert "CLIO_RELAY_GLOBUS_CLIENT_ID" in httpx.get(ask.url, timeout=30).text
    assert _completed(service, ask.ask_id) is None
    await service.close()


@pytest.mark.asyncio
async def test_a_grant_finishes_an_identity_ask(
    relay_paths: RelayPaths, tmp_path: Path
) -> None:
    grant = tmp_path / "host" / "globus.json"
    add_site(relay_paths, identity="globus")
    paths = RelayPaths(relay_paths.config_dir, relay_paths.state_dir, {"globus": grant})
    service = RelayService(paths)
    provider = provider_of(service)
    ask = await _identity_ask(service, "globus")
    assert not await provider.satisfied(ask)
    write_grant(grant, "globus", "from-the-host")
    provider.on_grant_change("globus")
    eventually(lambda: _completed(service, ask.ask_id), "the ask finished by the grant")
    found = provider.token_store().get("identity:globus")
    assert found is None
    probes = eventually(lambda: probe_module.load(paths).get("fx"), "the probe")
    assert probes["state"] == "ready"
    await service.close()


@pytest.mark.asyncio
async def test_satisfied_reads_the_grant(relay_paths: RelayPaths, tmp_path: Path) -> None:
    grant = tmp_path / "globus.json"
    add_site(relay_paths)
    paths = RelayPaths(relay_paths.config_dir, relay_paths.state_dir, {"globus": grant})
    service = RelayService(paths)
    provider = provider_of(service)
    ask = await _identity_ask(service, "globus")
    write_grant(grant, "globus", "from-the-host")
    assert await provider.satisfied(ask)
    assert _completed(service, ask.ask_id) is not None
    await service.close()


# GitHub's device flow, against a stub


class GitHubStub:
    def __init__(self, answers: list[str]) -> None:
        self.answers = answers
        self.polls = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        assert form["client_id"] == ["stub-github-client"]
        if request.url.path == "/login/device/code":
            return httpx.Response(
                200,
                json={
                    "device_code": "dev-code",
                    "user_code": "WDJB-MJHT",
                    "verification_uri": f"{GITHUB}/login/device",
                    "interval": 0.05,
                    "expires_in": 900,
                },
            )
        assert request.url.path == "/login/oauth/access_token"
        assert form["device_code"] == ["dev-code"]
        answer = self.answers[min(self.polls, len(self.answers) - 1)]
        self.polls += 1
        if answer == "done":
            return httpx.Response(
                200, json={"access_token": "gho_x", "token_type": "bearer", "scope": "repo"}
            )
        return httpx.Response(200, json={"error": answer})


@pytest.mark.asyncio
async def test_github_device_sign_in(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_GITHUB_CLIENT_ID", "stub-github-client")
    monkeypatch.setenv("CLIO_RELAY_GITHUB_URL", GITHUB)
    monkeypatch.setattr(signin, "SLOW_DOWN_STEP_S", 0.05)
    add_site(relay_paths)
    service = RelayService(relay_paths)
    provider = provider_of(service)
    stub = GitHubStub(["authorization_pending", "slow_down", "authorization_pending", "done"])
    _stubbed(provider, stub.handler)
    ask = await _identity_ask(service, "github")
    assert ask.url is not None
    assert submit(ask.url, "start").status_code == 303
    page = httpx.get(ask.url, timeout=30)
    if _completed(service, ask.ask_id) is None:
        assert "WDJB-MJHT" in page.text
    eventually(lambda: _completed(service, ask.ask_id), "the device sign-in")
    assert stub.polls == 4
    stored = service.asks.get(ask.ask_id)
    assert stored is not None and "WDJB-MJHT" not in stored.model_dump_json()
    github = provider.identities().get("github")
    assert github is not None
    token = await github.token("api.github.com")
    assert token is not None and token.access_token.get_secret_value() == "gho_x"
    await service.close()


@pytest.mark.asyncio
async def test_github_declined_on_github(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_GITHUB_CLIENT_ID", "stub-github-client")
    monkeypatch.setenv("CLIO_RELAY_GITHUB_URL", GITHUB)
    add_site(relay_paths)
    service = RelayService(relay_paths)
    provider = provider_of(service)
    _stubbed(provider, GitHubStub(["access_denied"]).handler)
    ask = await _identity_ask(service, "github")
    assert ask.url is not None
    assert submit(ask.url, "start").status_code == 303
    eventually(
        lambda: "declined on GitHub" in httpx.get(ask.url or "", timeout=30).text, "the refusal"
    )
    current = service.asks.get(ask.ask_id)
    assert current is not None and current.state == "pending" and current.completed_at is None
    await service.close()


@pytest.mark.asyncio
async def test_github_poll_states() -> None:
    answers = ["authorization_pending", "slow_down", "expired_token", "access_denied", "done"]
    stub = GitHubStub(answers)
    context = IdentityContext(
        store=TokenStore("memory"),
        grants=GrantFiles(),
        settings={"github": {"client_id": "stub-github-client", "url": GITHUB}},
    )
    github = GitHubIdentity(context)
    async with httpx.AsyncClient(transport=httpx.MockTransport(stub.handler)) as http:
        ctx = SignInContext(store=context.store, http=http)
        step = await github.begin(ctx)
        assert step.relay_page and step.display["code"] == "WDJB-MJHT"
        assert "device_code" not in step.model_dump()
        states = [(await github.poll(ctx, step)).state for _ in answers]
    assert states == ["pending", "slow_down", "failed", "failed", "done"]


# Probing


@pytest.mark.asyncio
async def test_probe_classifies_sites(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(probe_module.catalog, "sites", list)
    add_site(relay_paths, "ready", identity="globus")
    add_site(relay_paths, "locked", identity="globus", config={"sign_in": True})
    add_site(relay_paths, "other", identity="github")
    results = await probe_module.probe_identity("globus", relay_paths)
    assert {item.site: item.state for item in results} == {
        "ready": "ready",
        "locked": "needs_sign_in",
    }
    assert set(probe_module.load(relay_paths)) == {"ready", "locked"}


@pytest.mark.parametrize(
    ("refusal", "state"),
    [
        (Refusal(RefusalReason.UNREACHABLE, "no allocation", allocation=False), "missing_allocation"),
        (Refusal(RefusalReason.NEEDS_SIGN_IN, "x"), "needs_sign_in"),
        (Refusal(RefusalReason.INVALID_CONFIG, "x"), "not_configured"),
        (Refusal(RefusalReason.UNREACHABLE, "x"), "unreachable"),
    ],
)
def test_probe_refusal_states(refusal: Refusal, state: str) -> None:
    assert probe_module._classify(refusal) == state  # pyright: ignore[reportPrivateUsage]


def test_provider_builds_nothing_until_used(relay_paths: RelayPaths) -> None:
    service = RelayService(relay_paths)
    provider = provider_of(service)
    assert provider._loopback is None  # pyright: ignore[reportPrivateUsage]
    assert provider._watcher is None  # pyright: ignore[reportPrivateUsage]
    assert not (relay_paths.state_dir / "pages").exists()

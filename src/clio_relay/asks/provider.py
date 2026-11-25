"""Relay's ask provider: W1's ``asks`` hook (RL25a, RL27 to RL30).

Every ask is durable, so any path can answer it: the client's elicitation, the
task's ``input_required``, the RL2a refusal with a page address, relay's page,
or ``clio-relay asks``. This provider adds relay's page, the sign-ins behind
URL asks, the askpass prompts, grant files and probing.

Building it starts nothing. The page server, the grant watcher and the askpass
listener start the first time something needs them, so a CLI process that only
reads asks stays quiet.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx

from clio_relay import identity
from clio_relay.adapters.base import SignInNeed
from clio_relay.adapters.hooks import (
    Ask,
    AskState,
    ChoiceOption,
    HookContext,
    approve_schema,
    choice_ask,
    short_ask,
)
from clio_relay.asks import askpass
from clio_relay.asks.core import PageCore
from clio_relay.asks.loopback import LoopbackServer
from clio_relay.asks.pages import DEFAULT_PAGE_TTL_S, PageTokens
from clio_relay.asks.runner import Runner
from clio_relay.asks.signin import Mode, SignIns, mode_of
from clio_relay.identity import GrantFiles, GrantWatcher, Identities, IdentityContext, TokenStore
from clio_relay.identity import get as identity_factory
from clio_relay.identity.probe import probe_identity
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import OperationRecord, RecordStore, new_id, write_json_atomic
from clio_relay.jobs.refusals import Refusal
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.settings import RelayPaths

logger = logging.getLogger(__name__)

PUBLIC_URL_ENV = "CLIO_RELAY_PUBLIC_URL"
PAGE_PORT_ENV = "CLIO_RELAY_PAGE_PORT"
PAGE_TTL_ENV = "CLIO_RELAY_PAGE_TTL_S"


def base_file(state_dir: Path) -> Path:
    """Return where the serving process writes its page address."""
    return state_dir / "pages" / "base.json"


def read_base(state_dir: Path) -> str | None:
    """Return the page address a serving relay wrote, if any."""
    try:
        value: object = json.loads(base_file(state_dir).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if isinstance(value, dict):
        base = value.get("base")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if isinstance(base, str):
            return base
    return None


def page_url(base: str, ask_id: str, token: str) -> str:
    """Return a page address."""
    return f"{base.rstrip('/')}/relay/asks/{ask_id}?t={token}"


def _token_of(url: str | None) -> str:
    if not url:
        return ""
    return (parse_qs(urlsplit(url).query).get("t") or [""])[0]


class RelayAskProvider:
    """Durable asks, relay's page and the sign-ins behind it."""

    def __init__(self, context: HookContext) -> None:
        self.context = context
        settings = context.settings
        ttl = settings.get("page_ttl_s") or os.environ.get(PAGE_TTL_ENV) or DEFAULT_PAGE_TTL_S
        self.tokens = PageTokens(context.state_dir / "pages", float(ttl))
        self.runner = Runner()
        self.signins = SignIns(self)
        self.core = PageCore(self)
        self._lock = threading.Lock()
        self._store: TokenStore | None = None
        self._identities: Identities | None = None
        self._grants: GrantFiles | None = None
        self._http: httpx.AsyncClient | None = None
        self._loopback: LoopbackServer | None = None
        self._public_base: str | None = None
        self._watcher: GrantWatcher | None = None
        self._listening = False
        self._operations: RecordStore[OperationRecord] | None = None

    # Shared pieces, made on first use

    def token_store(self) -> TokenStore:
        """Return relay's store: the keyring, or the private file in the config directory."""
        config_dir = self.paths().config_dir
        with self._lock:
            if self._store is None:
                backend = self.context.settings.get("keyring")
                self._store = TokenStore(
                    backend if isinstance(backend, str) else None, config_dir=config_dir
                )
            return self._store

    def grants(self) -> GrantFiles:
        """Return the grant files the host keeps current."""
        with self._lock:
            if self._grants is None:
                self._grants = GrantFiles.from_settings(self.context.settings)
            return self._grants

    def identities(self) -> Identities:
        """Return the identity providers."""
        grants = self.grants()
        store = self.token_store()
        with self._lock:
            if self._identities is None:
                self._identities = Identities(
                    IdentityContext(store=store, grants=grants, settings=self.context.settings)
                )
                identity.use(self._identities)
            return self._identities

    def http(self) -> httpx.AsyncClient:
        """Return the HTTP client sign-ins use. Call on the runner's loop."""
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=False)
        return self._http

    def paths(self) -> RelayPaths:
        """Return relay's paths, with this provider's state directory."""
        configured = self.context.settings.get("config_dir")
        config = (
            Path(configured) if isinstance(configured, str) else RelayPaths.from_env().config_dir
        )
        return RelayPaths(
            config_dir=config,
            state_dir=self.context.state_dir,
            grant_files={key: Path(value) for key, value in self.grants().paths.items()},
        )

    def operations(self) -> RecordStore[OperationRecord]:
        """Return the operation records."""
        directory = self.context.state_dir / "operations"
        with self._lock:
            if self._operations is None:
                self._operations = RecordStore(directory, OperationRecord)
            return self._operations

    # Where the page is

    def base_url(self) -> str:
        """Return the page's base address, starting the loopback page if needed."""
        if self._public_base is not None:
            return self._public_base
        with self._lock:
            if self._loopback is None:
                port = int(os.environ.get(PAGE_PORT_ENV) or 0)
                self._loopback = LoopbackServer(self, port=port)
            loopback = self._loopback
        base = loopback.base
        self._announce(base)
        return base

    def _announce(self, base: str) -> None:
        if read_base(self.context.state_dir) != base:
            write_json_atomic(base_file(self.context.state_dir), {"base": base, "pid": os.getpid()})
        self.start_background()

    def callback_url(self) -> str:
        """Return where a redirect sign-in comes back to."""
        return f"{self.base_url()}/relay/asks/callback"

    def mount(self, server: Any) -> None:
        """Serve the page under relay's own origin, beside the MCP endpoint (HTTP only)."""
        from clio_relay.asks.routes import mount

        base = self.context.settings.get("public_base_url") or os.environ.get(PUBLIC_URL_ENV)
        if not isinstance(base, str) or not base:
            logger.warning("no public address for relay's page; it stays on loopback")
            return
        mount(server, self, base)
        self._public_base = base.rstrip("/")
        self._announce(self._public_base)

    def start_background(self) -> None:
        """Start the askpass listener and the grant watcher, once."""
        with self._lock:
            if self._listening:
                return
            self._listening = True
        askpass.broker().add_listener(self.signins.on_prompt)
        watcher = GrantWatcher(self.grants(), self.on_grant_change)
        watcher.start()
        self._watcher = watcher

    # Asks

    def _put(self, ask: Ask) -> Ask:
        self.context.asks.put(ask.ask_id, ask)
        return ask

    async def choose(
        self,
        *,
        site: SiteEntry,
        operation_id: str | None,
        message: str,
        field_name: str,
        options: Sequence[str | ChoiceOption],
        title: str | None = None,
        recommended_reason: str | None = None,
    ) -> Ask:
        return self._put(
            short_ask(
                choice_ask(
                    ask_id=new_id("ask"),
                    site=site.id,
                    operation_id=operation_id,
                    message=message,
                    field_name=field_name,
                    options=options,
                    title=title,
                    recommended_reason=recommended_reason,
                )
            )
        )

    async def approve(
        self,
        *,
        site: SiteEntry,
        operation_id: str | None,
        message: str,
        binding: dict[str, Any],
        plain: bool = True,
    ) -> Ask:
        ask = Ask(
            ask_id=new_id("ask"),
            kind="approve",
            site=site.id,
            operation_id=operation_id,
            message=message,
            requested_schema=approve_schema(plain=plain),
        )
        ask.bind(binding)
        return self._put(short_ask(ask))

    def _mode(self, site: SiteEntry, need: SignInNeed) -> Mode:
        if need.url:
            return "external"
        if need.provider.startswith("keyring:"):
            return "keyring"
        if identity_factory(need.provider) is not None:
            return "identity"
        if askpass.starter(site.id) is not None:
            return "askpass"
        return "confirm"

    async def url(self, *, site: SiteEntry, operation_id: str | None, need: SignInNeed) -> Ask:
        mode = self._mode(site, need)
        return self.store_url_ask(
            ask_id=new_id("ask"),
            site=site.id,
            operation_id=operation_id,
            message=need.message,
            binding={"provider": need.provider, "mode": mode, "relay_page": mode != "external"},
            url=need.url,
        )

    def store_url_ask(
        self,
        *,
        ask_id: str,
        site: str,
        operation_id: str | None,
        message: str,
        binding: dict[str, Any],
        url: str | None = None,
    ) -> Ask:
        """Record a URL ask. Without ``url`` it points at relay's page."""
        ask = Ask(
            ask_id=ask_id,
            kind="url",
            site=site,
            operation_id=operation_id,
            message=message,
            binding=binding,
        )
        ask.url = url or page_url(self.base_url(), ask_id, self.tokens.mint(ask_id))
        return self._put(ask)

    async def satisfied(self, ask: Ask) -> bool:
        current = self.context.asks.get(ask.ask_id) or ask
        if current.completed_at is not None:
            return True
        if mode_of(current) != "identity":
            return False
        identity = self.identities().get(str(current.binding.get("provider", "")))
        if identity is None:
            return False
        try:
            status = await identity.status()
        except Refusal:
            return False
        if status.state != "signed_in":
            return False
        self.finish_url_ask(current.ask_id, {"signed_in": True, "source": status.source})
        return True

    def page_address(self, ask: Ask) -> str | None:
        if ask.kind == "url" and mode_of(ask) == "external":
            return ask.url
        if ask.kind == "url":
            if self.tokens.check(ask.ask_id, _token_of(ask.url)) == "ok":
                return ask.url
            fresh = page_url(self.base_url(), ask.ask_id, self.tokens.mint(ask.ask_id))
            with self.context.asks.locked(ask.ask_id):
                stored = self.context.asks.get(ask.ask_id)
                if stored is not None:
                    stored.url = fresh
                    self.context.asks.put(ask.ask_id, stored)
            return fresh
        return page_url(self.base_url(), ask.ask_id, self.tokens.mint(ask.ask_id))

    def resolve(self, ask_id: str, state: AskState, answer: dict[str, Any]) -> Ask | None:
        """Record an answer from the page, once."""
        with self.context.asks.locked(ask_id):
            ask = self.context.asks.get(ask_id)
            if ask is None:
                return None
            if ask.state == "pending":
                ask.resolve(state, answer, "page")
            elif ask.kind == "url" and state == "declined" and ask.completed_at is None:
                # A client may have said "open it" before the person declined here.
                ask.state = "declined"
                ask.answer = answer
                ask.answered_by = "page"
                ask.answered_at = utc_now()
            self.context.asks.put(ask_id, ask)
            return ask

    def finish_url_ask(self, ask_id: str, answer: dict[str, Any]) -> None:
        """Mark a URL ask's sign-in finished, and kill its addresses."""
        with self.context.asks.locked(ask_id):
            ask = self.context.asks.get(ask_id)
            if ask is not None and ask.state in ("pending", "accepted"):
                ask.resolve("accepted", {"action": "accept", "content": answer}, "page")
                if ask.completed_at is None:
                    ask.completed_at = utc_now()
                self.context.asks.put(ask_id, ask)
        self.tokens.spend(ask_id)

    # Grants and probing

    def on_grant_change(self, provider: str) -> None:
        """A host's grant file changed: probe what it unlocks."""
        logger.info("the %s grant changed", provider)
        self.after_sign_in(provider)

    def after_sign_in(self, provider: str) -> None:
        """Probe the sites an identity unlocks, in the background."""
        asyncio.run_coroutine_threadsafe(self._after(provider), self.runner.loop())

    async def _after(self, provider: str) -> None:
        identity = self.identities().get(provider)
        if identity is not None:
            try:
                status = await identity.status()
            except Refusal:
                status = None
            if status is not None and status.state == "signed_in":
                for ask in self.context.asks.all():
                    if (
                        ask.kind == "url"
                        and ask.completed_at is None
                        and ask.state in ("pending", "accepted")
                        and mode_of(ask) == "identity"
                        and ask.binding.get("provider") == provider
                    ):
                        self.finish_url_ask(
                            ask.ask_id, {"signed_in": True, "source": status.source}
                        )
        try:
            results = await probe_identity(provider, self.paths())
        except Exception:
            logger.exception("probing the sites %s unlocks failed", provider)
            return
        for item in results:
            logger.info("probe %s via %s: %s", item.site, provider, item.state)

    async def close(self) -> None:
        if self._watcher is not None:
            self._watcher.stop()
            self._watcher = None
        if self._listening:
            askpass.broker().remove_listener(self.signins.on_prompt)
            self._listening = False
        self.signins.close()
        if self._identities is not None:
            identity.release(self._identities)
        if self._loopback is not None:
            self._loopback.close()
            self._loopback = None
        client, self._http = self._http, None
        if client is not None:
            with contextlib.suppress(RuntimeError):
                await self.runner.call(client.aclose())
        self.runner.close()

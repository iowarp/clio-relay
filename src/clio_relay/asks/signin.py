"""The sign-ins behind URL asks (RL28, RL30).

A URL ask is in one of these modes, chosen when it is made and kept in its
binding:

* ``external``: the adapter gave its own address; relay only waits.
* ``identity``: a provider in the identity table signs the person in (Globus,
  GitHub, TACC). A redirect sign-in returns to ``/relay/asks/callback``; a
  device sign-in is polled here; a pasted code goes in a box on the page.
* ``askpass``: a sign-in registered by the site's adapter runs, and each ssh
  prompt it raises shows on the page.
* ``prompt``: an ssh call that was not under a sign-in raised a prompt.
* ``keyring``: the page takes a password and keeps it in the keyring.
* ``confirm``: the person signs in elsewhere and says so.

None of this sets a time limit on the person.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from clio_relay.asks import askpass
from clio_relay.asks.askpass import AskpassSession, Prompt
from clio_relay.identity import device
from clio_relay.identity.base import SignInContext, SignInStep
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal

if TYPE_CHECKING:
    from clio_relay.adapters.hooks import Ask
    from clio_relay.asks.provider import RelayAskProvider

logger = logging.getLogger(__name__)

Mode = Literal["external", "identity", "keyring", "askpass", "prompt", "confirm"]
SignInState = Literal["idle", "running", "done", "failed"]
MODES: tuple[Mode, ...] = ("external", "identity", "keyring", "askpass", "prompt", "confirm")


@dataclass
class SignIn:
    """One URL ask's sign-in, in this process's memory."""

    ask_id: str
    mode: Mode
    provider: str
    site: str
    operation_id: str | None
    state: SignInState = "idle"
    message: str | None = None
    step: SignInStep | None = None
    session: AskpassSession | None = None
    tasks: list[asyncio.Task[Any]] = field(default_factory=list["asyncio.Task[Any]"])


def mode_of(ask: Ask) -> Mode:
    """Return the mode a URL ask was made in."""
    value = ask.binding.get("mode")
    if value in MODES:
        return value  # pyright: ignore[reportReturnType]
    return "external" if ask.url and not ask.binding.get("relay_page") else "confirm"


class SignIns:
    """Runs the sign-ins of one provider's URL asks."""

    def __init__(self, owner: RelayAskProvider) -> None:
        self.owner = owner
        self._items: dict[str, SignIn] = {}
        self._lock = threading.Lock()

    def get(self, ask: Ask) -> SignIn:
        """Return an ask's sign-in, made on first use."""
        with self._lock:
            found = self._items.get(ask.ask_id)
            if found is None:
                found = SignIn(
                    ask_id=ask.ask_id,
                    mode=mode_of(ask),
                    provider=str(ask.binding.get("provider", "")),
                    site=ask.site,
                    operation_id=ask.operation_id,
                )
                if ask.completed_at is not None:
                    found.state = "done"
                self._items[ask.ask_id] = found
            return found

    def all(self) -> list[SignIn]:
        """Return every sign-in this process holds."""
        with self._lock:
            return list(self._items.values())

    def _fail(self, item: SignIn, message: str) -> None:
        item.state = "failed"
        item.message = message

    async def complete(self, item: SignIn, answer: dict[str, Any] | None = None) -> None:
        """Mark the sign-in and its ask finished."""
        item.state = "done"
        item.message = None
        self.owner.finish_url_ask(item.ask_id, answer or {"signed_in": True})
        if item.session is not None:
            askpass.broker().forget(item.session)
        if item.mode == "identity":
            self.owner.after_sign_in(item.provider)

    # Starting

    async def start(self, ask: Ask) -> None:
        """Start (or restart) an ask's sign-in. Runs on the provider's loop."""
        item = self.get(ask)
        if item.state in ("running", "done"):
            return
        for task in item.tasks:
            task.cancel()
        item.tasks.clear()
        item.state = "running"
        item.message = None
        if item.mode == "identity":
            await self._start_identity(item)
        elif item.mode == "askpass":
            await self._start_askpass(item)

    def _context(self, item: SignIn) -> SignInContext:
        return SignInContext(
            store=self.owner.token_store(),
            http=self.owner.http(),
            redirect_uri=self.owner.callback_url(),
            step=item.step,
        )

    async def _start_identity(self, item: SignIn) -> None:
        identity = self.owner.identities().get(item.provider)
        if identity is None:
            self._fail(item, f"relay has no {item.provider} sign-in")
            return
        ctx = self._context(item)
        try:
            step = await identity.begin(ctx)
        except Refusal as refusal:
            self._fail(item, refusal.message)
            return
        item.step = step
        ctx.step = step
        if device.polls(step):
            item.tasks.append(self.owner.runner.spawn(self._poll(item, ctx)))

    async def _poll(self, item: SignIn, ctx: SignInContext) -> None:
        identity = self.owner.identities().get(item.provider)
        step = item.step
        if identity is None or step is None:
            return

        def new_step(fresh: SignInStep) -> None:
            item.step = fresh
            item.message = "The code expired, so relay asked for a new one."

        def error(refusal: Refusal) -> None:
            item.message = refusal.message

        try:
            result = await device.follow(
                identity,
                ctx,
                step,
                running=lambda: item.state == "running",
                on_new_step=new_step,
                on_error=error,
            )
        except Refusal as refusal:
            self._fail(item, refusal.message)
            return
        if result is None:
            return
        if result.state == "done":
            await self.complete(item)
        else:
            self._fail(item, result.message or "the sign-in did not finish")

    async def _start_askpass(self, item: SignIn) -> None:
        start = askpass.starter(item.site)
        if start is None:
            self._fail(item, f"relay has no way to start a sign-in to {item.site}")
            return
        session = askpass.broker().new_session(item.site, item.operation_id)
        session.ask_id = item.ask_id
        item.session = session
        env = askpass.environment_for(session, self.owner.context.state_dir)
        item.tasks.append(self.owner.runner.spawn(self._run_starter(item, start, env)))

    async def _run_starter(self, item: SignIn, start: askpass.Starter, env: dict[str, str]) -> None:
        try:
            await start(env)
        except Refusal as refusal:
            self._fail(item, refusal.message)
        except Exception as exc:
            logger.exception("the sign-in to %s failed", item.site)
            detail = str(exc).strip()[:300]
            reason = f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__
            self._fail(item, f"the sign-in failed: {reason}")
        else:
            await self.complete(item)
            return
        if item.session is not None:
            askpass.broker().forget(item.session)
            item.session = None

    # Answers from the page

    async def callback(self, params: dict[str, str]) -> SignIn | None:
        """Finish the redirect sign-in whose OAuth state matches."""
        wanted = params.get("state", "")
        if not wanted:
            return None
        for item in self.all():
            step = item.step
            if item.mode != "identity" or step is None or item.state != "running":
                continue
            if hmac.compare_digest(str(step.data.get("state", "")), wanted):
                await self._finish_identity(item, params)
                return item
        return None

    async def code(self, ask: Ask, code: str) -> None:
        """Finish a sign-in with a code the person pasted."""
        item = self.get(ask)
        if item.mode != "identity" or item.step is None or item.state != "running":
            return
        await self._finish_identity(item, {"code": code})

    async def _finish_identity(self, item: SignIn, params: dict[str, str]) -> None:
        identity = self.owner.identities().get(item.provider)
        if identity is None:
            self._fail(item, f"relay has no {item.provider} sign-in")
            return
        try:
            result = await identity.callback(self._context(item), params)
        except Refusal as refusal:
            self._fail(item, refusal.message)
            return
        if result.state == "done":
            await self.complete(item)
        elif result.state == "failed":
            self._fail(item, result.message or "the sign-in did not finish")

    async def secret(self, ask: Ask, value: str) -> None:
        """Keep a password the person typed, for a keyring ask."""
        item = self.get(ask)
        name = item.provider.removeprefix("keyring:")
        try:
            self.owner.token_store().set_secret(name, value)
        except Refusal as refusal:
            self._fail(item, refusal.message)
            return
        await self.complete(item, {"stored": name})

    async def prompt(self, ask: Ask, prompt_id: str, value: str | None) -> bool:
        """Answer (or, with ``None``, refuse) one ssh prompt on an ask's page."""
        item = self.get(ask)
        session = item.session
        if session is None:
            return False
        done = session.decline(prompt_id) if value is None else session.answer(prompt_id, value)
        if item.mode == "prompt" and done and not session.open_prompts():
            item.state = "done"
            self.owner.finish_url_ask(item.ask_id, {"answered": session.answered_count})
        return done

    async def confirm(self, ask: Ask) -> None:
        """The person says they finished signing in elsewhere."""
        await self.complete(self.get(ask))

    # Prompts from ssh calls that were not under a sign-in

    def on_prompt(self, session: AskpassSession, prompt: Prompt) -> None:
        """Give a prompt a page. Called on the broker's thread."""
        if session.ask_id is not None:
            with self._lock:
                item = self._items.get(session.ask_id)
            if item is not None and item.state != "done":
                return
        ask_id = new_id("ask")
        ask = self.owner.store_url_ask(
            ask_id=ask_id,
            site=session.site,
            operation_id=session.operation_id,
            message=f"{session.title or session.site} is asking for a sign-in.",
            binding={"provider": "ssh", "mode": "prompt", "relay_page": True},
        )
        with self._lock:
            self._items[ask_id] = SignIn(
                ask_id=ask_id,
                mode="prompt",
                provider="ssh",
                site=session.site,
                operation_id=session.operation_id,
                state="running",
                session=session,
            )
        session.ask_id = ask.ask_id
        logger.info("an ssh prompt for %s waits on ask %s", session.site, ask.ask_id)

    def close(self) -> None:
        """Refuse every open ssh prompt. The runner cancels the tasks."""
        for item in self.all():
            item.state = "failed" if item.state == "running" else item.state
            if item.session is not None:
                askpass.broker().forget(item.session)

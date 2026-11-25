"""What relay's page shows and does, for the loopback server and the HTTP routes.

``GET`` shows an ask and changes nothing. ``POST`` must carry the page token,
the form token for it, an ``Origin`` (or ``Referer``) of the page itself, and
the operation and binding digest the page showed. An approval also checks that
the operation still waits on exactly that job and charge. Field values are
never logged.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

from clio_relay.adapters.hooks import Ask
from clio_relay.asks.pages import (
    PageResponse,
    PageTokens,
    binding_table,
    e,
    field_name,
    hidden,
    option_rows,
    options,
    response,
)
from clio_relay.asks.signin import SignIn, mode_of
from clio_relay.jobs.records import OperationRecord, OperationState, RecordStore
from clio_relay.jobs.spec import canonical_digest

if TYPE_CHECKING:
    from clio_relay.asks.provider import RelayAskProvider

MAX_FIELD = 8 * 1024


def origin_ok(headers: Mapping[str, str], allowed: set[str]) -> bool:
    """Return whether a POST could have come from relay's own page.

    The form's CSRF token is the defence. This adds the browser's word: a
    cross-site ``Sec-Fetch-Site`` or a foreign ``Origin`` or ``Referer`` is
    refused. A client that sends none of them (curl, a script holding the
    page's token) is not a browser being tricked, so it passes.
    """
    fetch_site = headers.get("sec-fetch-site")
    if fetch_site is not None and fetch_site not in ("same-origin", "none"):
        return False
    origin = headers.get("origin")
    if origin and origin != "null":
        return origin.rstrip("/") in allowed
    referer = headers.get("referer")
    if referer:
        parts = urlsplit(referer)
        return f"{parts.scheme}://{parts.netloc}" in allowed
    return origin != "null"


def _link(url: str, label: str) -> str:
    if not url.startswith(("https://", "http://")):
        return ""
    return f"<p><a href='{e(url)}' rel='noopener noreferrer' target='_blank'>{e(label)}</a></p>"


def _button(action: str, label: str, extra: str = "") -> str:
    return f"<button name='action' value='{e(action)}'{extra}>{e(label)}</button>"


def approval_covers(binding: dict[str, Any], record: OperationRecord) -> bool:
    """Say whether an approval's binding covers ``record``.

    An approval is bound to one job's digest, except one with ``scope: site``
    (installing relay's jarvis-cd there, say), which covers every operation on
    that site, so operations waiting on the same thing share one ask.
    """
    if binding.get("scope") == "site":
        return binding.get("site") == record.site
    return binding.get("spec_digest") == record.spec_digest


def stale_approval(ask: Ask, operations: RecordStore[OperationRecord]) -> str | None:
    """Return why an approval no longer matches its operation, or ``None``.

    An approval is bound to the operation, the job's hash and the charge. A
    site-scoped one is checked against an operation still waiting on it.
    """
    if canonical_digest(ask.binding) != ask.binding_digest:
        return "The approval's details were changed."
    if not ask.operation_id:
        return None
    record = operations.get(ask.operation_id)
    if ask.binding.get("scope") == "site":
        waiting = [
            other
            for other in operations.all()
            if ask.ask_id in other.asks and other.state == OperationState.AWAITING_INPUT
        ]
        record = waiting[0] if waiting else record
    if record is None:
        return "The operation is gone."
    if record.state != OperationState.AWAITING_INPUT:
        return f"The operation is already {record.state.value}."
    if not approval_covers(ask.binding, record):
        return "The job changed since this was asked."
    charge: Any = ask.binding.get("charge")
    if charge is not None and (record.result or {}).get("charge") != charge:
        return "The charge changed since this was asked."
    return None


class PageCore:
    """Relay's page for one provider."""

    def __init__(self, owner: RelayAskProvider) -> None:
        self.owner = owner

    @property
    def tokens(self) -> PageTokens:
        return self.owner.tokens

    # GET

    async def get(self, ask_id: str, token: str) -> PageResponse:
        """Show an ask."""
        check = self.tokens.check(ask_id, token)
        if check == "expired":
            return self._expired(ask_id)
        ask = self.owner.context.asks.get(ask_id) if check == "ok" else None
        if ask is None:
            return self._unknown()
        return self.render(ask, token)

    def _expired(self, ask_id: str) -> PageResponse:
        return response(
            410,
            "This address has expired",
            "<p>The question is still open. Get a fresh address with "
            f"<code>clio-relay asks page {e(ask_id)}</code>, or ask the agent to call "
            "<code>run</code> again with <code>resume</code>.</p>",
        )

    def _unknown(self) -> PageResponse:
        return response(
            404,
            "This address does not work",
            "<p>It was already used, or it is not one of relay's.</p>",
        )

    def _done(self, ask: Ask, text: str | None = None) -> PageResponse:
        if text is None:
            text = {
                "accepted": "Done. You can close this page.",
                "declined": "Declined. Relay will not go ahead.",
                "cancelled": "Cancelled.",
            }.get(ask.state, "Done. You can close this page.")
            if ask.kind == "url" and ask.completed_at is not None and ask.state != "declined":
                text = "Signed in. You can close this page."
        return response(200, "Relay", f"<p>{e(ask.message)}</p><p>{e(text)}</p>")

    @staticmethod
    def finished(ask: Ask) -> bool:
        """Return whether nothing more can be done on an ask's page."""
        if ask.kind == "url":
            return ask.completed_at is not None or ask.state in ("declined", "cancelled")
        return ask.state != "pending"

    def render(self, ask: Ask, token: str) -> PageResponse:
        """Return the page for an ask."""
        if self.finished(ask):
            return self._done(ask)
        form = self.tokens.form_token(ask.ask_id, token)
        fields = hidden(ask, token, form)
        intro = f"<p>{e(ask.message)}</p><p class='note'>Site: {e(ask.site)}</p>"
        if ask.kind == "approve":
            body = (
                intro
                + binding_table(ask)
                + f"<form method='post'>{fields}"
                + _button("accept", "Approve")
                + _button("decline", "Decline")
                + "</form>"
            )
            return response(200, "Relay asks for approval", body)
        if ask.kind == "choose":
            choices = option_rows(ask)
            body = (
                intro
                + f"<form method='post'>{fields}{choices}"
                + _button("accept", "Choose")
                + _button("decline", "Decline")
                + "</form>"
            )
            return response(200, "Relay asks you to choose", body)
        return self._render_url(ask, intro, fields)

    def _decline_form(self, fields: str) -> str:
        return f"<form method='post'>{fields}{_button('decline', 'Decline')}</form>"

    def _render_url(self, ask: Ask, intro: str, fields: str) -> PageResponse:
        item = self.owner.signins.get(ask)
        failed = f"<p><strong>{e(item.message)}</strong></p>" if item.message else ""
        refresh: int | None = None
        title = "Relay needs you to sign in"
        if item.mode == "identity":
            identity = self.owner.identities().get(item.provider)
            label = identity.title if identity is not None else item.provider
            body, refresh = self._identity(item, label, fields)
        elif item.mode in ("askpass", "prompt"):
            body, refresh = self._prompts(item, fields)
        elif item.mode == "keyring":
            body = (
                f"<form method='post'>{fields}<label>Password<br>"
                "<input type='password' name='value' autocomplete='off' autofocus></label><br>"
                f"{_button('secret', 'Save')}</form>"
            )
        elif item.mode == "external":
            body = (
                _link(ask.url or "", "Open the sign-in page")
                + f"<form method='post'>{fields}{_button('done', 'I have finished')}</form>"
            )
            refresh = 10
        else:
            body = f"<form method='post'>{fields}{_button('done', 'I have finished')}</form>"
        page_body = intro + failed + body + self._decline_form(fields)
        return response(200, title, page_body, refresh_s=refresh)

    def _identity(self, item: SignIn, label: str, fields: str) -> tuple[str, int | None]:
        step = item.step
        if item.state != "running" or step is None:
            if item.state == "running":
                return "<p>Starting the sign-in.</p>", 2
            start = _button("start", f"Sign in with {label}")
            return f"<form method='post'>{fields}{start}</form>", None
        if not step.relay_page:
            code = step.display.get("code")
            shown = f"<p class='code'>{e(code)}</p>" if code else ""
            return (
                _link(step.url, f"Continue to {label}")
                + shown
                + "<p class='note'>Come back here when you have signed in.</p>",
                5,
            )
        parts = [f"<p>{e(step.display.get('instructions', ''))}</p>"]
        link = step.display.get("link") or step.url
        parts.append(_link(link, f"Open {label}"))
        code = step.display.get("code")
        if code:
            parts.append(f"<p class='code'>{e(code)}</p>")
        if step.accepts_code:
            parts.append(
                f"<form method='post'>{fields}<label>Code<br>"
                "<input type='text' name='value' autocomplete='off'></label><br>"
                f"{_button('code', 'Finish sign-in')}</form>"
            )
            return "".join(parts), None
        parts.append(f"<p class='note'>Relay is waiting for {e(label)}.</p>")
        return "".join(parts), 5

    def _prompts(self, item: SignIn, fields: str) -> tuple[str, int | None]:
        session = item.session
        if item.mode == "askpass" and item.state in ("idle", "failed"):
            return f"<form method='post'>{fields}{_button('start', 'Start sign-in')}</form>", None
        prompts = session.open_prompts() if session is not None else []
        if not prompts:
            return "<p class='note'>Waiting for the site.</p>", 3
        parts: list[str] = []
        for prompt in prompts:
            pid = f"<input type='hidden' name='prompt_id' value='{e(prompt.prompt_id)}'>"
            text = f"<p><strong>{e(prompt.text or 'The site asks for an answer.')}</strong></p>"
            if prompt.kind == "notice":
                parts.append(text + "<p class='note'>Relay goes on once that is done.</p>")
                continue
            if prompt.kind == "confirm":
                controls = _button("prompt_yes", "Yes") + _button("prompt_decline", "No")
            else:
                kind = "password" if prompt.kind == "secret" else "text"
                controls = (
                    f"<input type='{kind}' name='value' autocomplete='off' autofocus><br>"
                    + _button("prompt", "Send")
                    + _button("prompt_decline", "Refuse")
                )
            parts.append(f"{text}<form method='post'>{fields}{pid}{controls}</form>")
        return "".join(parts), None

    # POST

    async def post(
        self,
        ask_id: str,
        fields: Mapping[str, str],
        headers: Mapping[str, str],
        allowed_origins: set[str],
    ) -> PageResponse:
        """Act on a page's form."""
        token = fields.get("t", "")
        check = self.tokens.check(ask_id, token)
        if check == "expired":
            return self._expired(ask_id)
        if check != "ok" or not self.tokens.check_form(ask_id, token, fields.get("csrf", "")):
            return self._unknown()
        if not origin_ok(headers, allowed_origins):
            return response(403, "Refused", "<p>This did not come from relay's page.</p>")
        ask = self.owner.context.asks.get(ask_id)
        if ask is None:
            return self._unknown()
        # The page names what it showed. A form for another operation or another
        # binding is out of date; the approval itself is checked again below.
        if ("op" in fields and fields["op"] != (ask.operation_id or "")) or (
            "bd" in fields and fields["bd"] != ask.binding_digest
        ):
            return response(409, "This page is out of date", "<p>Reload it and look again.</p>")
        if self.finished(ask):
            return self._done(ask)
        action = fields.get("action", "")
        if any(len(value) > MAX_FIELD for value in fields.values()):
            return response(400, "Too long", "<p>That answer is too long.</p>")
        if action == "decline":
            ask = self.owner.resolve(ask_id, "declined", {"action": "decline"}) or ask
            self.tokens.spend(ask_id)
            return self._done(ask)
        if ask.kind in ("approve", "choose"):
            return self._answer(ask, action, fields)
        return await self._url_action(ask, action, fields, token)

    def _stale_job(self, ask: Ask) -> str | None:
        """Return why an approval no longer matches its operation, or ``None``."""
        return stale_approval(ask, self.owner.operations())

    def _answer(self, ask: Ask, action: str, fields: Mapping[str, str]) -> PageResponse:
        if action != "accept":
            return response(400, "Bad request", "<p>Choose an answer.</p>")
        if ask.kind == "approve":
            stale = self._stale_job(ask)
            if stale is not None:
                return response(
                    409, "Nothing was approved", f"<p>{e(stale)} Ask the agent to try again.</p>"
                )
            content: dict[str, Any] = {"approve": True}
        else:
            value = fields.get("value", "")
            if value not in options(ask):
                return response(400, "Bad request", "<p>Pick one of the choices.</p>")
            content = {field_name(ask): value}
        stored = self.owner.resolve(
            ask.ask_id, "accepted", {"action": "accept", "content": content}
        )
        self.tokens.spend(ask.ask_id)
        return self._done(stored or ask)

    async def _url_action(
        self, ask: Ask, action: str, fields: Mapping[str, str], token: str
    ) -> PageResponse:
        signins = self.owner.signins
        value = fields.get("value", "")
        if action == "accept" and mode_of(ask) in ("confirm", "external"):
            action = "done"
        if action == "start":
            await signins.start(ask)
        elif action == "code" and value.strip():
            await signins.code(ask, value.strip())
        elif action == "secret" and value:
            await signins.secret(ask, value)
        elif action in ("prompt", "prompt_yes", "prompt_decline"):
            answer = (
                None if action == "prompt_decline" else ("yes" if action == "prompt_yes" else value)
            )
            await signins.prompt(ask, fields.get("prompt_id", ""), answer)
        elif action == "done":
            await signins.confirm(ask)
        else:
            return response(400, "Bad request", "<p>Relay did not understand that.</p>")
        current = self.owner.context.asks.get(ask.ask_id) or ask
        if self.finished(current):
            return self._done(current)
        return PageResponse(
            303,
            "",
            {"Location": f"{quote(ask.ask_id)}?t={quote(token)}", "Cache-Control": "no-store"},
        )

    # The OAuth redirect back

    async def callback(self, params: Mapping[str, str]) -> PageResponse:
        """Finish a redirect sign-in."""
        item = await self.owner.signins.callback(dict(params))
        if item is None:
            return response(
                404, "Nothing is waiting", "<p>Relay is not waiting for this sign-in.</p>"
            )
        if item.state == "done":
            return response(200, "Signed in", "<p>Signed in. You can close this page.</p>")
        return response(
            400, "The sign-in did not finish", f"<p>{e(item.message or 'Try again.')}</p>"
        )

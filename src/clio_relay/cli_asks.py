"""``clio-relay asks``: see and answer what relay is asking, and manage sign-ins (RL28, RL30).

Every answer goes through the same durable record the page and the client use,
so it counts once whichever path answers first.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import TYPE_CHECKING, Annotated, Any, NoReturn, cast

import typer

from clio_relay.adapters.hooks import Ask, ask_detail
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.refusals import Refusal
from clio_relay.surface.settings import RelayPaths

if TYPE_CHECKING:
    from clio_relay.identity import Identities
    from clio_relay.surface.service import RelayService

app = typer.Typer(no_args_is_help=True, help="See and answer what relay is asking, and sign in.")


def _paths() -> RelayPaths:
    return RelayPaths.from_env()


def _print(value: Any) -> None:
    typer.echo(json.dumps(value, indent=2, sort_keys=True, default=str))


def _fail(message: str, code: int = 2) -> NoReturn:
    typer.echo(message, err=True)
    raise typer.Exit(code)


def _service() -> RelayService:
    from clio_relay.surface.service import RelayService

    return RelayService(_paths())


def _ask(service: RelayService, ask_id: str) -> Ask:
    found = service.asks.get(ask_id)
    if found is None:
        _fail(f"there is no ask {ask_id!r}")
    return found


def _waiting(ask: Ask) -> bool:
    return ask.state == "pending" or (
        ask.kind == "url" and ask.completed_at is None and ask.state == "accepted"
    )


@app.command("list")
def asks_list(
    all_asks: Annotated[bool, typer.Option("--all", help="Include answered asks")] = False,
) -> None:
    """List what relay is waiting on you for."""
    service = _service()
    _print(
        [
            {
                "ask_id": ask.ask_id,
                "kind": ask.kind,
                "site": ask.site,
                "operation_id": ask.operation_id,
                "message": ask.message,
                "detail": ask_detail(ask),
                "state": ask.state,
                "created_at": ask.created_at,
            }
            for ask in sorted(service.asks.all(), key=lambda item: item.created_at)
            if all_asks or _waiting(ask)
        ]
    )


@app.command("show")
def asks_show(ask_id: str) -> None:
    """Show one ask, with what an approval approves."""
    ask = _ask(_service(), ask_id)
    shown = ask.model_dump(mode="json", exclude={"url"})
    _print(shown)


def _advance(service: RelayService, ask: Ask) -> None:
    from clio_relay.surface.installing import holders

    held = holders(service, ask)
    if not held:
        _print({"ask": ask.ask_id, "state": ask.state})
        return
    records = [asyncio.run(service.advance(operation_id)) for operation_id in held]
    shown: dict[str, Any] = {
        "ask": ask.ask_id,
        "operation": records[0].operation_id,
        "state": records[0].state.value,
    }
    if len(records) > 1:
        shown["also"] = [
            {"operation": record.operation_id, "state": record.state.value}
            for record in records[1:]
        ]
    _print(shown)


@app.command("answer")
def asks_answer(
    ask_id: str,
    value: Annotated[str | None, typer.Option(help="The choice, for a choose ask")] = None,
    comment: Annotated[str | None, typer.Option(help="What to tell the agent beside it")] = None,
) -> None:
    """Approve, choose, or say a sign-in is finished."""
    from clio_relay.asks.core import stale_approval
    from clio_relay.asks.pages import field_name, options

    service = _service()
    ask = _ask(service, ask_id)
    content: dict[str, Any] | None = None
    if ask.kind == "approve":
        stale = stale_approval(ask, service.operations)
        if stale is not None and ask.state == "pending":
            _fail(f"approval_mismatch: {stale} Nothing was approved.")
        content = {"approve": True}
    elif ask.kind == "choose":
        if value not in options(ask):
            _fail(f"choose one of: {', '.join(options(ask))}")
        content = {field_name(ask): value}
    try:
        ask = service.answer(ask_id, "accept", content, by="cli", comment=comment)
        if ask.kind == "url" and ask.completed_at is None and ask.state == "accepted":
            with service.asks.locked(ask_id):
                stored = service.asks.get(ask_id)
                if stored is not None:
                    stored.completed_at = utc_now()
                    service.asks.put(ask_id, stored)
                    ask = stored
        _advance(service, ask)
    except Refusal as refusal:
        _fail(f"{refusal.reason.value}: {refusal.message}")


@app.command("decline")
def asks_decline(
    ask_id: str,
    comment: Annotated[str | None, typer.Option(help="What to tell the agent beside it")] = None,
) -> None:
    """Say no. The operation waiting on it is cancelled."""
    service = _service()
    _ask(service, ask_id)
    try:
        _advance(service, service.answer(ask_id, "decline", None, by="cli", comment=comment))
    except Refusal as refusal:
        _fail(f"{refusal.reason.value}: {refusal.message}")


@app.command("page")
def asks_page(ask_id: str) -> None:
    """Print a fresh address of relay's page for one ask."""
    from clio_relay.asks.pages import DEFAULT_PAGE_TTL_S, PageTokens
    from clio_relay.asks.provider import PAGE_TTL_ENV, page_url, read_base
    from clio_relay.asks.routes import operator_token_file

    paths = _paths()
    service = _service()
    ask = _ask(service, ask_id)
    base = read_base(paths.state_dir)
    if base is None or not _page_answers(base):
        if base is not None and not _loopback(base):
            _fail(f"relay's page at {base} is not answering. Check the relay serving it")
        _serve_page(service, ask)
        return
    ttl = float(os.environ.get(PAGE_TTL_ENV) or DEFAULT_PAGE_TTL_S)
    token = PageTokens(paths.state_dir / "pages", ttl).mint(ask.ask_id)
    typer.echo(page_url(base, ask.ask_id, token))
    if not _loopback(base):
        where = operator_token_file(paths.state_dir)
        typer.echo(f"Sign in with any user name and the operator token in {where}", err=True)


def _loopback(base: str) -> bool:
    return base.startswith(("http://127.0.0.1", "http://localhost"))


def _page_answers(base: str) -> bool:
    """Return whether the page a relay recorded still answers.

    The record outlives its writer: a relay that exited, or one its host stopped,
    leaves an address nothing answers.
    """
    import httpx

    try:
        httpx.get(f"{base.rstrip('/')}/relay/asks/", timeout=3.0)
    except httpx.HTTPError:
        return False
    return True


def _serve_page(service: RelayService, ask: Ask) -> None:
    """Serve relay's page from this command until the ask is answered."""
    from clio_relay.asks.provider import base_file, read_base

    address = service.ask_provider.page_address(ask)
    if address is None:
        _fail("no relay is serving its page here, and this relay has no page to serve")
    typer.echo(address)
    typer.echo(
        "No running relay is serving its page, so this command serves it until the ask "
        "is answered. Press Ctrl+C to stop.",
        err=True,
    )
    mine = read_base(service.paths.state_dir)
    try:
        while True:
            current = service.asks.get(ask.ask_id)
            if current is None or not _waiting(current):
                break
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        asyncio.run(service.close())
        if mine is not None and read_base(service.paths.state_dir) == mine:
            base_file(service.paths.state_dir).unlink(missing_ok=True)


def _identities() -> Identities:
    from clio_relay.identity import GrantFiles, Identities, IdentityContext, TokenStore

    paths = _paths()
    return Identities(
        IdentityContext(
            store=TokenStore(config_dir=paths.config_dir),
            grants=GrantFiles.from_settings({"grant_files": dict(paths.grant_files)}),
        )
    )


@app.command("identities")
def asks_identities() -> None:
    """Show each sign-in relay knows, and where its token comes from."""
    from clio_relay.identity.probe import load

    statuses = asyncio.run(_identities().statuses())
    probes: dict[str, dict[str, Any]] = {
        site: cast(dict[str, Any], item)
        for site, item in load(_paths()).items()
        if isinstance(item, dict)
    }
    _print(
        [
            {
                **status.model_dump(mode="json", exclude_none=True),
                "sites": {
                    site: item.get("state")
                    for site, item in probes.items()
                    if item.get("identity") == status.provider
                },
            }
            for status in statuses
        ]
    )


async def _sign_in(name: str) -> str:
    import httpx

    from clio_relay.identity import SignInContext, SignInStep, device

    identities = _identities()
    identity = identities.get(name)
    if identity is None:
        _fail(f"relay has no {name} sign-in")
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as http:
        ctx = SignInContext(store=identities.context.store, http=http)
        step = await identity.begin(ctx)
        ctx.step = step
        typer.echo(step.display.get("instructions") or "Open this address and sign in:")
        typer.echo(step.display.get("link") or step.url)
        if step.display.get("code"):
            typer.echo(f"Code: {step.display['code']}")
        if step.accepts_code:
            code = typer.prompt("Code", hide_input=True).strip()
            result = await identity.callback(ctx, {"code": code})
        else:

            def new_step(fresh: SignInStep) -> None:
                typer.echo("The code expired. Here is a new one.")
                typer.echo(fresh.display.get("link") or fresh.url)
                if fresh.display.get("code"):
                    typer.echo(f"Code: {fresh.display['code']}")

            def error(refusal: Refusal) -> None:
                typer.echo(f"{refusal.message}. Trying again.", err=True)

            found = await device.follow(identity, ctx, step, on_new_step=new_step, on_error=error)
            if found is None:
                _fail("the sign-in did not finish")
            result = found
    if result.state != "done":
        _fail(result.message or "the sign-in did not finish")
    return result.message or "Signed in."


@app.command("sign-in")
def asks_sign_in(provider: str) -> None:
    """Sign in to an identity from this terminal (Globus, GitHub, TACC)."""
    from clio_relay.identity.probe import probe_identity

    try:
        typer.echo(asyncio.run(_sign_in(provider)))
        results = asyncio.run(probe_identity(provider, _paths()))
    except Refusal as refusal:
        _fail(f"{refusal.reason.value}: {refusal.message}")
    except KeyboardInterrupt:
        sys.exit(130)
    _print([item.model_dump(mode="json") for item in results])


@app.command("sign-out")
def asks_sign_out(provider: str) -> None:
    """Forget relay's own sign-in. A host's grant file stays the host's."""
    identity = _identities().get(provider)
    if identity is None:
        _fail(f"relay has no {provider} sign-in")
    asyncio.run(identity.sign_out())
    _print({"provider": provider, "signed_out": True})


@app.command("probe")
def asks_probe(provider: str) -> None:
    """Try every site an identity unlocks, and say which work."""
    from clio_relay.identity.probe import probe_identity

    started = time.monotonic()
    results = asyncio.run(probe_identity(provider, _paths()))
    _print(
        {
            "provider": provider,
            "seconds": round(time.monotonic() - started, 2),
            "sites": [item.model_dump(mode="json") for item in results],
        }
    )

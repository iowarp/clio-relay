"""``clio-relay-next``: the new relay surface, beside the old one during the rework."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import sys
from pathlib import Path
from typing import Annotated, Any

import typer
import yaml

from clio_relay import catalog
from clio_relay.adapters import registry
from clio_relay.jobs.refusals import Refusal
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.routing import routes
from clio_relay.sites.store import SiteStore
from clio_relay.surface.settings import RelayPaths

app = typer.Typer(no_args_is_help=True, help="Relay: run work on the sites you use.")
site_app = typer.Typer(no_args_is_help=True, help="Add, edit and remove sites.")
asks_app = typer.Typer(no_args_is_help=True, help="See and answer what relay is asking.")
app.add_typer(site_app, name="site")
app.add_typer(asks_app, name="asks")


def _paths() -> RelayPaths:
    return RelayPaths.from_env()


def _print(value: Any) -> None:
    typer.echo(json.dumps(value, indent=2, sort_keys=True, default=str))


def _fail(refusal: Refusal) -> None:
    typer.echo(f"{refusal.reason.value}: {refusal.message}", err=True)
    raise typer.Exit(2)


@app.command()
def serve(
    http: Annotated[bool, typer.Option(help="Serve Streamable HTTP instead of stdio")] = False,
    host: str = "127.0.0.1",
    port: int = 8766,
    path: str = "/mcp",
    token_file: Annotated[
        Path | None, typer.Option(help="File holding the HTTP bearer token")
    ] = None,
) -> None:
    """Serve relay as an MCP server."""
    from clio_relay.surface.server import create_server

    paths = _paths()
    if not http:
        create_server(paths, transport="stdio").run(transport="stdio", show_banner=False)
        return
    token = os.environ.get("CLIO_RELAY_TOKEN")
    if token is None:
        target = token_file or paths.state_dir / "http-token"
        if target.is_file():
            token = target.read_text(encoding="utf-8").strip()
        else:
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            token = secrets.token_urlsafe(32)
            target.write_text(token, encoding="utf-8")
            target.chmod(0o600)
        typer.echo(f"bearer token in {target}", err=True)
    normalized = "/" + path.strip("/")
    public = "127.0.0.1" if host in ("0.0.0.0", "::") else host
    server = create_server(
        paths, transport="http", token=token, base_url=f"http://{public}:{port}{normalized}"
    )
    server.run(transport="http", host=host, port=port, path=normalized, show_banner=False)


@app.command()
def adapters() -> None:
    """List installed adapters and their verbs."""
    found: dict[str, Any] = {}
    for name in registry.names():
        try:
            found[name] = sorted(registry.load(name).implements)
        except Refusal as refusal:
            found[name] = {"unavailable": refusal.reason.value, "message": refusal.message}
    _print(found)


@site_app.command("list")
def site_list() -> None:
    """List your sites."""
    _print([entry.model_dump(mode="json") for entry in SiteStore(_paths().sites_file).list()])


@site_app.command("show")
def site_show(site_id: str) -> None:
    """Show one site and which adapter serves each verb."""
    try:
        entry = SiteStore(_paths().sites_file).get(site_id)
    except Refusal as refusal:
        _fail(refusal)
        return
    _print(
        {
            "site": entry.model_dump(mode="json"),
            "routes": [item.model_dump(mode="json") for item in routes(entry)],
        }
    )


@site_app.command("add")
def site_add(
    site_id: str,
    adapter: Annotated[str, typer.Option(help="The adapter in use")],
    config: Annotated[str, typer.Option(help="Adapter settings, as JSON or YAML")] = "{}",
    name: str | None = None,
    policy: Annotated[str, typer.Option(help="open or guarded")] = "guarded",
    spend: Annotated[str, typer.Option(help="always, never, or a node-hour threshold")] = "always",
) -> None:
    """Add a site."""
    try:
        settings = yaml.safe_load(config) or {}
        registry.validate_config(adapter, settings)
        ask: Any = spend if spend in ("always", "never") else float(spend)
        entry = SiteEntry.model_validate(
            {
                "id": site_id,
                "name": name,
                "adapter": adapter,
                "config": settings,
                "policy": policy,
                "spend": {"ask": ask},
            }
        )
        _print(SiteStore(_paths().sites_file).add(entry).model_dump(mode="json"))
    except Refusal as refusal:
        _fail(refusal)


@site_app.command("catalog")
def site_catalog() -> None:
    """List the sites relay ships."""
    _print([entry.model_dump(mode="json") for entry in catalog.sites()])


@site_app.command("add-from-catalog")
def site_add_from_catalog(catalog_id: str, site_id: str | None = None) -> None:
    """Copy a catalog site into your sites file."""
    try:
        added = SiteStore(_paths().sites_file).add_from_catalog(catalog.site(catalog_id), site_id)
    except Refusal as refusal:
        _fail(refusal)
        return
    _print(added.model_dump(mode="json"))


@site_app.command("edit")
def site_edit(
    site_id: str,
    changes: Annotated[str, typer.Argument(help="Top-level field changes, as JSON or YAML")],
) -> None:
    """Change a site's fields."""
    try:
        loaded = yaml.safe_load(changes)
        if not isinstance(loaded, dict):
            typer.echo("changes must be a mapping", err=True)
            raise typer.Exit(2)
        edited = SiteStore(_paths().sites_file).edit(site_id, dict(loaded))  # pyright: ignore[reportUnknownArgumentType]
    except Refusal as refusal:
        _fail(refusal)
        return
    _print(edited.model_dump(mode="json"))


@site_app.command("remove")
def site_remove(site_id: str) -> None:
    """Remove a site."""
    try:
        removed = SiteStore(_paths().sites_file).remove(site_id)
    except Refusal as refusal:
        _fail(refusal)
        return
    _print(removed.model_dump(mode="json"))


@site_app.command("switch")
def site_switch(site_id: str, adapter: str) -> None:
    """Make an alternative adapter the one in use."""
    try:
        switched = SiteStore(_paths().sites_file).switch(site_id, adapter)
    except Refusal as refusal:
        _fail(refusal)
        return
    _print(switched.model_dump(mode="json"))


@asks_app.command("list")
def asks_list(
    all_asks: Annotated[bool, typer.Option("--all", help="Include answered asks")] = False,
) -> None:
    """List asks waiting on you."""
    from clio_relay.surface.service import RelayService

    service = RelayService(_paths())
    shown = [
        ask.model_dump(mode="json")
        for ask in service.asks.all()
        if all_asks or ask.state == "pending" or (ask.kind == "url" and ask.completed_at is None)
    ]
    _print(shown)


@asks_app.command("answer")
def asks_answer(
    ask_id: str,
    decline: Annotated[bool, typer.Option(help="Say no")] = False,
    value: Annotated[str | None, typer.Option(help="The choice, for a choose ask")] = None,
) -> None:
    """Answer an ask. Approves unless --decline."""
    from clio_relay.jobs.handles import utc_now
    from clio_relay.surface.service import RelayService

    service = RelayService(_paths())
    try:
        current = service.asks.get(ask_id)
        if current is None:
            typer.echo(f"there is no ask {ask_id!r}", err=True)
            raise typer.Exit(2)
        content: dict[str, Any] | None = None
        if current.kind == "approve":
            content = {"approve": not decline}
        elif current.kind == "choose":
            properties = (current.requested_schema or {}).get("properties") or {}
            field = next(iter(properties), "value")
            content = {field: value}
        ask = service.answer(ask_id, "decline" if decline else "accept", content, by="cli")
        if ask.kind == "url" and not decline and ask.completed_at is None:
            with service.asks.locked(ask_id):
                stored = service.asks.get(ask_id)
                if stored is not None:
                    stored.completed_at = utc_now()
                    service.asks.put(ask_id, stored)
        if ask.operation_id:
            record = asyncio.run(service.advance(ask.operation_id))
            _print({"ask": ask_id, "operation": record.operation_id, "state": record.state.value})
        else:
            _print({"ask": ask_id, "state": ask.state})
    except Refusal as refusal:
        _fail(refusal)


def main() -> None:
    """Entry point."""
    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)

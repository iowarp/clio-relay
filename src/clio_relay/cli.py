"""``clio-relay``: serve relay over MCP, and manage sites, asks and services."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import sys
from dataclasses import replace
from pathlib import Path
from typing import Annotated, Any, cast

import typer
import yaml

from clio_relay import catalog
from clio_relay.adapters import registry
from clio_relay.adapters.worker.cli import worker_app
from clio_relay.cli_asks import app as asks_app
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.routing import routes
from clio_relay.sites.store import SiteStore
from clio_relay.surface.settings import RelayPaths

app = typer.Typer(no_args_is_help=True, help="Relay: run work on the sites you use.")
site_app = typer.Typer(no_args_is_help=True, help="Add, edit and remove sites.")
rendezvous_app = typer.Typer(no_args_is_help=True, help="Where services meet relay's forwards.")
app.add_typer(site_app, name="site")
app.add_typer(rendezvous_app, name="rendezvous")


app.add_typer(asks_app, name="asks")
app.add_typer(worker_app, name="worker")


def _paths() -> RelayPaths:
    return RelayPaths.from_env()


def _print(value: Any) -> None:
    typer.echo(json.dumps(value, indent=2, sort_keys=True, default=str))


def _fail(refusal: Refusal) -> None:
    typer.echo(f"{refusal.reason.value}: {refusal.message}", err=True)
    raise typer.Exit(2)


def _grant_files(values: list[str]) -> dict[str, Path]:
    grants: dict[str, Path] = {}
    for value in values:
        provider, sep, target = value.partition("=")
        if not sep or not provider.strip() or not target.strip():
            typer.echo(f"--grant-file takes PROVIDER=PATH, not {value!r}", err=True)
            raise typer.Exit(2)
        grants[provider.strip().lower()] = Path(target.strip())
    return grants


@app.command()
def serve(
    http: Annotated[bool, typer.Option(help="Serve Streamable HTTP instead of stdio")] = False,
    host: str = "127.0.0.1",
    port: int = 8766,
    path: str = "/mcp",
    token_file: Annotated[
        Path | None, typer.Option(help="File holding the HTTP bearer token")
    ] = None,
    grant_file: Annotated[
        list[str] | None,
        typer.Option(help="PROVIDER=PATH: a grant file the host keeps current. Repeatable"),
    ] = None,
    tls_cert: Annotated[Path | None, typer.Option(help="TLS certificate, for HTTPS")] = None,
    tls_key: Annotated[Path | None, typer.Option(help="TLS private key, for HTTPS")] = None,
) -> None:
    """Serve relay as an MCP server."""
    from clio_relay.surface.server import create_server

    paths = _paths()
    if grant_file:
        paths = replace(paths, grant_files={**paths.grant_files, **_grant_files(grant_file)})
    if not http:
        create_server(paths, transport="stdio").run(transport="stdio", show_banner=False)
        return
    if (tls_cert is None) != (tls_key is None):
        typer.echo("give both --tls-cert and --tls-key, or neither", err=True)
        raise typer.Exit(2)
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
    origin = f"{'https' if tls_cert else 'http'}://{public}:{port}"
    server = create_server(
        paths,
        transport="http",
        token=token,
        base_url=f"{origin}{normalized}",
        public_base_url=origin,
    )
    # Page URLs carry single-use tokens, so requests are never logged.
    uvicorn_config: dict[str, Any] = {"access_log": False}
    if tls_cert is not None and tls_key is not None:
        uvicorn_config.update(ssl_certfile=str(tls_cert), ssl_keyfile=str(tls_key))
    from clio_relay.shutdown import register_server

    register_server(
        paths, server, f"{origin}{normalized}", origin, tls_cert=tls_cert if tls_key else None
    )
    server.run(
        transport="http",
        host=host,
        port=port,
        path=normalized,
        show_banner=False,
        uvicorn_config=uvicorn_config,
    )


@app.command()
def connect(
    handle: Annotated[str, typer.Argument(help="The handle in the serve result's connect_command")],
) -> None:
    """Forward a running service's ports to this machine and hold them open."""
    try:
        from clio_relay.serve import (  # pyright: ignore[reportMissingImports]
            connect as connect_service,  # pyright: ignore[reportUnknownVariableType]
        )
    except ModuleNotFoundError as exc:
        _no_service_runtime(exc)
        return
    try:
        connect_service(handle, paths=_paths())
    except Refusal as refusal:
        _fail(refusal)


@rendezvous_app.command("show")
def rendezvous_show() -> None:
    """Show the rendezvous settings."""
    _print(_rendezvous(None))


@rendezvous_app.command("set")
def rendezvous_set(
    values: Annotated[list[str], typer.Argument(help="KEY=VALUE settings to change")],
) -> None:
    """Change rendezvous settings."""
    changes: dict[str, str] = {}
    for value in values:
        key, sep, setting = value.partition("=")
        if not sep or not key.strip():
            typer.echo(f"settings are KEY=VALUE, not {value!r}", err=True)
            raise typer.Exit(2)
        changes[key.strip()] = setting.strip()
    _print(_rendezvous(changes))


def _rendezvous(changes: dict[str, str] | None) -> Any:
    try:
        from clio_relay.serve import (  # pyright: ignore[reportMissingImports]
            settings as rendezvous_settings,  # pyright: ignore[reportUnknownVariableType]
        )
    except ModuleNotFoundError as exc:
        _no_service_runtime(exc)
        return None
    try:
        return rendezvous_settings(_paths(), changes)  # pyright: ignore[reportUnknownVariableType]
    except Refusal as refusal:
        _fail(refusal)
        return None


def _no_service_runtime(exc: ModuleNotFoundError) -> None:
    if exc.name not in ("clio_relay.serve", "clio_relay"):
        raise exc
    typer.echo("verb_not_implemented: the service runtime is not installed", err=True)
    raise typer.Exit(2)


def _json_flag(extra: list[str] | None) -> None:
    """Accept the trailing ``+json``; relay's commands print JSON either way."""
    unknown = [item for item in extra or [] if item != "+json"]
    if unknown:
        typer.echo(f"unexpected arguments: {' '.join(unknown)}", err=True)
        raise typer.Exit(2)


@app.command()
def shutdown(
    extra: Annotated[list[str] | None, typer.Argument(help="+json, accepted")] = None,
    site: Annotated[str | None, typer.Option(help="Stop one site's relay processes")] = None,
    all_sites: Annotated[
        bool, typer.Option("--all", help="Stop every site's, and relay on this host")
    ] = False,
    timeout_s: Annotated[float | None, typer.Option(help="Seconds per site; 15 by default")] = None,
) -> None:
    """Stop relay's long-lived processes. Jobs keep running and are listed."""
    from clio_relay.shutdown import shutdown as stop_all
    from clio_relay.surface.service import RelayService

    _json_flag(extra)
    if (site is None) == (not all_sites):
        typer.echo("give exactly one of --site S or --all", err=True)
        raise typer.Exit(2)

    async def go() -> dict[str, Any]:
        service = RelayService(_paths())
        try:
            return await stop_all(
                service, {"all": all_sites or None, "site": site, "timeout_s": timeout_s}
            )
        finally:
            await service.close()

    try:
        _print(asyncio.run(go()))
    except Refusal as refusal:
        _fail(refusal)


@app.command()
def cancel(
    operation: Annotated[str, typer.Argument(help="The operation id run or serve returned")],
    extra: Annotated[list[str] | None, typer.Argument(help="+json, accepted")] = None,
) -> None:
    """Cancel a job from run, or stop a service from serve, the way the cancel tool does."""
    from clio_relay.surface.service import RelayService

    _json_flag(extra)

    async def go() -> dict[str, Any]:
        service = RelayService(_paths())
        try:
            return service.view(await service.cancel(operation))
        finally:
            await service.close()

    try:
        _print(asyncio.run(go()))
    except Refusal as refusal:
        _fail(refusal)


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


@app.command()
def describe(
    site_id: Annotated[str | None, typer.Argument(help="A site; all sites when left out")] = None,
) -> None:
    """Describe a site the way the describe tool does, which checks relay reaches it."""
    from clio_relay.surface.service import RelayService

    async def go() -> dict[str, Any]:
        service = RelayService(_paths())
        try:
            return await service.describe(site_id)
        finally:
            await service.close()

    try:
        _print(asyncio.run(go()))
    except Refusal as refusal:
        _fail(refusal)


@site_app.command("list")
def site_list() -> None:
    """List your sites."""
    _print([entry.model_dump(mode="json") for entry in SiteStore(_paths().sites_file).list()])


def _saved(entry: SiteEntry) -> None:
    """Print a saved site and what its adapters set up for it."""

    async def set_up() -> dict[str, Any]:
        from clio_relay.surface.service import RelayService

        service = RelayService(_paths())
        try:
            return await service.site_added(entry.id)
        finally:
            await service.close()

    shown = entry.model_dump(mode="json")
    set_up_by = asyncio.run(set_up())
    if set_up_by:
        shown["set_up"] = set_up_by
    _print(shown)


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


@site_app.command("defaults")
def site_defaults(
    adapter: Annotated[
        str | None, typer.Option(help="The adapter a new site uses when --adapter is left out")
    ] = None,
    policy: Annotated[
        str | None, typer.Option(help="open or guarded, for a new site without --policy")
    ] = None,
    spend: Annotated[
        str | None,
        typer.Option(
            help="always, never, or a node-hour threshold, for a new site without --spend"
        ),
    ] = None,
) -> None:
    """Show the defaults for new sites, or change them."""
    store = SiteStore(_paths().sites_file)
    try:
        if adapter is None and policy is None and spend is None:
            shown = store.defaults()
        else:
            if adapter is not None:
                registry.load(adapter)
            shown = store.set_defaults(
                adapter=adapter,
                policy=policy,
                spend=None if spend is None else _spend_ask(spend),
            )
    except Refusal as refusal:
        _fail(refusal)
        return
    _print(shown.model_dump(mode="json"))


def _spend_ask(text: str) -> str | float:
    """Read ``--spend``: always, never, or a node-hour threshold above zero."""
    if text in ("always", "never"):
        return text
    try:
        value = float(text)
    except ValueError:
        value = 0.0
    if value <= 0:
        typer.echo("--spend takes always, never, or a number of node-hours above 0", err=True)
        raise typer.Exit(2)
    return value


@site_app.command("add")
def site_add(
    site_id: str,
    adapter: Annotated[
        str | None, typer.Option(help="The adapter in use; `site defaults` when left out")
    ] = None,
    config: Annotated[str, typer.Option(help="Adapter settings, as JSON or YAML")] = "{}",
    name: str | None = None,
    policy: Annotated[
        str | None, typer.Option(help="open or guarded; `site defaults` when left out")
    ] = None,
    spend: Annotated[
        str | None,
        typer.Option(help="always, never, or a node-hour threshold; `site defaults` when left out"),
    ] = None,
    skill: Annotated[
        str | None, typer.Option(help="A skill name, or the path of your own Markdown skill")
    ] = None,
) -> None:
    """Add a site."""
    store = SiteStore(_paths().sites_file)
    try:
        defaults = store.defaults()
        adapter = adapter or defaults.adapter
        loaded: Any = yaml.safe_load(config) or {}
        if not isinstance(loaded, dict):
            typer.echo("config must be a mapping", err=True)
            raise typer.Exit(2)
        settings = cast(dict[str, Any], loaded)
        registry.validate_config(adapter, settings)
        ask = defaults.spend if spend is None else _spend_ask(spend)
        entry = SiteEntry.model_validate(
            {
                "id": site_id,
                "name": name,
                "adapter": adapter,
                "config": settings,
                "policy": policy or defaults.policy,
                "spend": {"ask": ask},
                "skill": skill,
            }
        )
        added = store.add(entry)
    except Refusal as refusal:
        _fail(refusal)
        return
    _saved(added)


@site_app.command("catalog")
def site_catalog(
    schema: Annotated[bool, typer.Option(help="Print the schema of a catalog site file")] = False,
) -> None:
    """List the sites relay ships."""
    if schema:
        _print(catalog.schema())
        return
    _print([entry.model_dump(mode="json") for entry in catalog.sites()])


@site_app.command("add-from-catalog")
def site_add_from_catalog(catalog_id: str, site_id: str | None = None) -> None:
    """Copy a catalog site into your sites file."""
    try:
        added = SiteStore(_paths().sites_file).add_from_catalog(catalog.site(catalog_id), site_id)
    except Refusal as refusal:
        _fail(refusal)
        return
    _saved(added)


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
    _saved(edited)


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
    _saved(switched)


@site_app.command("clean")
def site_clean(
    site_id: str,
    extra: Annotated[list[str] | None, typer.Argument(help="+json, accepted")] = None,
    yes: Annotated[bool, typer.Option("--yes", help="Skip the ask: you approve here")] = False,
    remove_spack: Annotated[
        bool,
        typer.Option(
            help="Delete the Spack store and JARVIS's Miniforge too; rebuilding them takes hours"
        ),
    ] = False,
    forget_decisions: Annotated[
        bool,
        typer.Option(help="Delete JARVIS's recorded install answers; they are asked again"),
    ] = False,
    remove_outputs: Annotated[
        bool,
        typer.Option(help="Delete what relay's runs wrote outside its directories too"),
    ] = False,
    wait_s: Annotated[float, typer.Option(help="How long to wait for the answer")] = 600.0,
) -> None:
    """Remove relay's deployment on a site, so the next use deploys it again.

    It stops relay's processes there first, and keeps the Spack store, JARVIS's
    recorded install answers and the runs' outputs outside relay's directories
    unless told.
    """
    from clio_relay import site_clean as cleaning
    from clio_relay.surface.service import RelayService

    _json_flag(extra)

    async def approved(service: RelayService, planned: dict[str, Any]) -> bool:
        if yes:
            return True
        ask = await service.ask_provider.approve(
            site=service.site(site_id),
            operation_id=None,
            message=cleaning.describe_plan(planned),
            binding={"action": "site_clean", **planned},
            plain=False,
        )
        if service.asks.get(ask.ask_id) is None:
            service.asks.put(ask.ask_id, ask)
        if sys.stdin.isatty():
            if typer.confirm(ask.message, default=False, err=True):
                service.answer(ask.ask_id, "accept", {"approve": True}, by="cli")
            else:
                service.answer(ask.ask_id, "decline", by="cli")
        else:
            page = service.ask_provider.page_address(ask)
            where = f"Answer at {page}" if page else f"Answer: clio-relay asks answer {ask.ask_id}"
            typer.echo(f"waiting for ask {ask.ask_id}: {ask.message} {where}", err=True)
        deadline = asyncio.get_running_loop().time() + wait_s
        while True:
            current = service.asks.get(ask.ask_id)
            state = current.state if current is not None else "cancelled"
            if state != "pending":
                return state == "accepted"
            if asyncio.get_running_loop().time() > deadline:
                service.answer(ask.ask_id, "cancel", by="cli")
                return False
            await asyncio.sleep(1.0)

    async def go() -> dict[str, Any]:
        service = RelayService(_paths())
        try:
            planned = await cleaning.plan(
                service,
                site_id,
                remove_spack=remove_spack,
                forget_decisions=forget_decisions,
                remove_outputs=remove_outputs,
            )
            if not await approved(service, planned):
                raise Refusal(RefusalReason.POLICY_REFUSED, f"cleaning {site_id} was not approved")
            return await cleaning.clean(service, planned)
        finally:
            await service.close()

    try:
        _print(asyncio.run(go()))
    except Refusal as refusal:
        _fail(refusal)


def main() -> None:
    """Entry point."""
    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)

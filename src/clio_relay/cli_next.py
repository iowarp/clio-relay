"""``clio-relay-next``: the new relay surface, beside the old one during the rework."""

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
from clio_relay.cli_asks import app as asks_app
from clio_relay.jobs.refusals import Refusal
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
    server.run(
        transport="http",
        host=host,
        port=port,
        path=normalized,
        show_banner=False,
        uvicorn_config=uvicorn_config,
    )


@app.command()
def connect(handle: Annotated[str, typer.Argument(help="A service's operation id")]) -> None:
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
        loaded: Any = yaml.safe_load(config) or {}
        if not isinstance(loaded, dict):
            typer.echo("config must be a mapping", err=True)
            raise typer.Exit(2)
        settings = cast(dict[str, Any], loaded)
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


def main() -> None:
    """Entry point."""
    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)

"""The desktop side: ``clio-relay connect`` and the serve settings (RL20).

When relay runs away from the desktop, the endpoint it reports is on relay's
own host. The handle in its ``connect_command`` lets the desktop bind the same
ports itself, through the rendezvous host. The desktop's own rendezvous
setting supplies the token; the handle never carries it.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.serve import config as serve_settings
from clio_relay.serve.forward import Forward, port_bound
from clio_relay.serve.plan import PortPlan, ServePlan, free_loopback_port
from clio_relay.surface.settings import RelayPaths


def decode(handle: str) -> dict[str, Any]:
    """Return the payload of a connect handle."""
    try:
        padded = handle + "=" * (-len(handle) % 4)
        payload: Any = json.loads(base64.urlsafe_b64decode(padded.encode()))
    except ValueError as exc:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "that is not a connect handle") from exc
    if not isinstance(payload, dict) or payload.get("v") != 1:  # pyright: ignore[reportUnknownMemberType]
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS, "that connect handle is from another version"
        )
    return payload  # pyright: ignore[reportUnknownVariableType]


def visitor_plan(
    handle: str, rendezvous: serve_settings.Rendezvous, ports: dict[str, int] | None = None
) -> ServePlan:
    """Return the plan a desktop visitor needs, from a handle and the local setting."""
    payload = decode(handle)
    chosen = ports or {}
    listed: list[tuple[str, Any, int]] = [
        (str(name), protocol, int(local)) for name, protocol, local in payload["ports"]
    ]
    locals_: dict[str, int] = {}
    for name, _protocol, local in listed:
        wanted = chosen.get(name, local)
        if name not in chosen and port_bound(wanted):
            wanted = free_loopback_port()
        locals_[name] = wanted
    return ServePlan(
        operation_id=str(payload["op"]),
        site="connect",
        adapter="connect",
        service="connect",
        mode="frp",
        ports=[
            PortPlan(name=name, protocol=protocol, remote_port=0, local_port=locals_[name])
            for name, protocol, _local in listed
        ],
        secret_key=str(payload["sk"]),
        rendezvous=rendezvous.model_copy(update={"direct": bool(payload.get("direct"))}),
    )


async def hold(plan: ServePlan, directory: Path) -> None:
    """Hold the visitor until interrupted, binding the same ports again after a drop."""

    shown = 0

    def restarted(count: int, _pid: int | None) -> None:
        nonlocal shown
        if count > shown:
            shown = count
            print(f"the forward dropped and holds the same port again ({count})", file=sys.stderr)

    forward = Forward(plan, directory, restarted)
    stop = asyncio.Event()
    if os.name != "nt":
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGHUP):
            loop.add_signal_handler(signum, stop.set)
    await forward.start()
    try:
        while not forward.bound():
            await asyncio.sleep(1)
        for port in plan.ports:
            print(f"{port.name}: {port.url() or f'127.0.0.1:{port.local_port}'}", flush=True)
        said: str | None = None
        while not stop.is_set():
            note = forward.direct_note()
            if note and note != said:
                said = note
                print(note, file=sys.stderr, flush=True)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), 1)
    finally:
        await forward.close()


def connect(handle: str, *, paths: RelayPaths, ports: dict[str, int] | None = None) -> None:
    """Bind a served service's ports here and hold them until interrupted."""
    base = paths.state_dir
    loaded = serve_settings.load(serve_settings.settings_path(base))
    if loaded.rendezvous is None:
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            "set the rendezvous host on this machine first; see docs/rendezvous-host.md",
        )
    plan = visitor_plan(handle, loaded.rendezvous, ports)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(hold(plan, base / "serve" / "connect" / plan.operation_id))


_RENDEZVOUS_KEYS = set(serve_settings.Rendezvous.model_fields)
_SITE_KEYS = set(serve_settings.SiteServe.model_fields)


def settings(paths: RelayPaths, changes: dict[str, str] | None) -> dict[str, Any]:
    """Show the serve settings, or change them and show the result.

    Keys are the rendezvous fields (``address``, ``port``, ``protocol``,
    ``token_file`` and so on) or ``site.<id>.<field>`` for one site. An empty
    value clears a key. ``rendezvous=`` with an empty value clears the whole
    rendezvous setting.
    """
    path = serve_settings.settings_path(paths.state_dir)
    if not changes:
        return serve_settings.show(path)
    current = serve_settings.load(path)
    rendezvous: dict[str, Any] = (
        current.rendezvous.model_dump(exclude_none=True) if current.rendezvous else {}
    )
    sites: dict[str, dict[str, Any]] = {
        key: value.model_dump(exclude_defaults=True) for key, value in current.sites.items()
    }
    clear_rendezvous = False
    for key, value in changes.items():
        if key == "rendezvous" and not value:
            clear_rendezvous = True
            rendezvous = {}
            continue
        if key.startswith("site."):
            site_id, _, field = key[len("site.") :].rpartition(".")
            if not site_id or field not in _SITE_KEYS:
                raise Refusal(
                    RefusalReason.INVALID_ARGUMENTS,
                    f"unknown site setting {key!r}; fields are {', '.join(sorted(_SITE_KEYS))}",
                )
            _apply(sites.setdefault(site_id, {}), field, value)
            continue
        if key not in _RENDEZVOUS_KEYS:
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                f"unknown setting {key!r}; rendezvous fields are "
                f"{', '.join(sorted(_RENDEZVOUS_KEYS))}, and site.<id>.<field> sets a site",
            )
        clear_rendezvous = False
        _apply(rendezvous, key, value)
    try:
        updated = serve_settings.ServeSettings(
            rendezvous=None
            if clear_rendezvous and not rendezvous
            else serve_settings.Rendezvous.model_validate(rendezvous)
            if rendezvous
            else None,
            sites={
                key: serve_settings.SiteServe.model_validate(value) for key, value in sites.items()
            },
        )
    except ValidationError as exc:
        raise Refusal(RefusalReason.INVALID_CONFIG, f"invalid serve settings: {exc}") from exc
    serve_settings.save(path, updated)
    return serve_settings.show(path)


def _apply(target: dict[str, Any], key: str, value: str) -> None:
    if value == "":
        target.pop(key, None)
    elif value.lower() in ("true", "false"):
        target[key] = value.lower() == "true"
    else:
        target[key] = value


def main(argv: list[str] | None = None, *, state_dir: Path | None = None) -> int:
    """Run ``clio-relay connect`` on its own."""
    parser = argparse.ArgumentParser(prog="clio-relay connect", description=__doc__)
    parser.add_argument("handle")
    parser.add_argument(
        "--port",
        action="append",
        default=[],
        metavar="NAME=PORT",
        help="Bind a port on another local port",
    )
    parser.add_argument("--state-dir", type=Path, default=state_dir)
    args = parser.parse_args(argv)
    paths = RelayPaths.from_env()
    if args.state_dir is not None:
        paths = RelayPaths(paths.config_dir, args.state_dir, paths.grant_files)
    ports: dict[str, int] = {}
    for item in args.port:
        name, _, value = str(item).partition("=")
        if not value.isdigit():
            parser.error(f"--port takes NAME=PORT, not {item!r}")
        ports[name] = int(value)
    try:
        connect(args.handle, paths=paths, ports=ports)
    except Refusal as refusal:
        print(f"clio-relay connect: {refusal}", file=sys.stderr)
        return 2
    return 0

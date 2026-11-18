"""A terminal driver for the Tapis site steps a person takes once.

``python -m clio_relay.adapters.tapis <command> --site <id>`` reads the site
from relay's ``sites.yaml``. Commands:

- ``sign-in``: start the TACC device sign-in, show the TACC page and the code,
  and wait until the person approves it. The tokens go to the keyring and are
  never printed.
- ``status``: the TACC sign-in state, without tokens.
- ``lifetimes``: when each stored token was issued and expires, read from its
  claims. No token is printed.
- ``refresh``: refresh the access token now, to check the refresh grant.
- ``describe``: what the adapter reports for the site.
- ``register-host`` and ``unregister-host``: register or remove the site's
  Tapis system and the person's credential (RL48, RL49).

Relay's MCP tools run the same code; this exists so the live rows and a demo
can be driven step by step.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from typing import Any

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.tapis.adapter import TapisAdapter
from clio_relay.adapters.tapis.config import TapisConfig
from clio_relay.adapters.tapis.register import SshHost, register_host, unregister_host
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal
from clio_relay.sites.store import SiteStore
from clio_relay.surface.settings import RelayPaths


def _adapter(site_id: str) -> tuple[TapisAdapter, SshHost | None]:
    paths = RelayPaths.from_env()
    entry = SiteStore(paths.sites_file).get(site_id)
    if entry.adapter == "tapis":
        config = entry.config
    elif "tapis" in entry.alternatives:
        config = entry.alternatives["tapis"]
    else:
        raise SystemExit(f"site {site_id} has no tapis adapter")
    context = AdapterContext(
        site_id=entry.id,
        state_dir=paths.adapter_dir(entry.id, "tapis"),
        content=ContentStore(paths.content_dir),
    )
    adapter = TapisAdapter(TapisConfig.model_validate(config), context)
    ssh_config = entry.config if entry.adapter == "ssh" else entry.alternatives.get("ssh")
    ssh = SshHost.from_alternative(ssh_config) if ssh_config else None
    return adapter, ssh


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, default=str), flush=True)


def _say(text: str) -> None:
    # Flushed, so a person or a script reading a pipe sees the code at once.
    print(text, flush=True)


async def _sign_in(adapter: TapisAdapter) -> int:
    identity = adapter.identity
    if await identity.token() is not None:
        _say("Already signed in to TACC.")
        _print((await identity.status()).model_dump(mode="json"))
        return 0
    step = await identity.begin()
    _say(identity.message(step))
    _say(f"TACC page: {step.url}")
    _say("Waiting for the sign-in. Press Ctrl-C to stop.")
    while True:
        await asyncio.sleep(step.poll_interval_s or 5.0)
        result = await identity.poll()
        if result.state == "done":
            _say("Signed in.")
            _print((await identity.status()).model_dump(mode="json"))
            return 0
        if result.state == "expired":
            step = await identity.begin()
            _say("The code expired. Here is a new one.")
            _say(identity.message(step))
            _say(f"TACC page: {step.url}")
            continue
        if result.state == "failed":
            _say(f"The sign-in did not finish: {result.message}")
            return 1


async def _main(args: argparse.Namespace) -> int:
    adapter, ssh = _adapter(args.site)
    if args.command == "sign-in":
        return await _sign_in(adapter)
    if args.command == "sign-out":
        await adapter.identity.sign_out()
        _say("Signed out of TACC.")
        return 0
    if args.command == "status":
        _print((await adapter.identity.status()).model_dump(mode="json"))
        return 0
    if args.command == "lifetimes":
        _print(adapter.identity.lifetimes())
        return 0
    if args.command == "refresh":
        refreshed = await adapter.identity.refresh_now()
        _say("A new access token was issued." if refreshed else "No refresh was possible.")
        _print(adapter.identity.lifetimes())
        return 0 if refreshed else 1
    if args.command == "describe":
        _print((await adapter.describe()).model_dump(mode="json"))
        return 0
    if args.command == "register-host":
        _print(asdict(await register_host(adapter, ssh)))
        return 0
    if args.command == "unregister-host":
        _print(await unregister_host(adapter, ssh))
        return 0
    raise SystemExit(f"unknown command {args.command}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m clio_relay.adapters.tapis")
    parser.add_argument(
        "command",
        choices=[
            "sign-in",
            "sign-out",
            "status",
            "lifetimes",
            "refresh",
            "describe",
            "register-host",
            "unregister-host",
        ],
    )
    parser.add_argument("--site", required=True, help="The site's id in sites.yaml")
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_main(args))
    except Refusal as refusal:
        _print(refusal.to_dict())
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())

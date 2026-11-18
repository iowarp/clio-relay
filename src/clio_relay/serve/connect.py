"""``clio-relay connect <handle>``: reach a service a remote relay serves (RL20).

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
import sys
from pathlib import Path
from typing import Any

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.serve import settings as serve_settings
from clio_relay.serve.forward import Forward
from clio_relay.serve.plan import PortPlan, ServePlan
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
    return ServePlan(
        operation_id=str(payload["op"]),
        site="connect",
        adapter="connect",
        service="connect",
        mode="frp",
        ports=[
            PortPlan(
                name=name, protocol=protocol, remote_port=0, local_port=chosen.get(name, local)
            )
            for name, protocol, local in listed
        ],
        secret_key=str(payload["sk"]),
        rendezvous=rendezvous.model_copy(update={"direct": bool(payload.get("direct"))}),
    )


async def hold(plan: ServePlan, directory: Path) -> None:
    """Hold the visitor until interrupted, binding the same ports again after a drop."""

    def restarted(count: int, _pid: int | None) -> None:
        if count:
            print(f"the forward ended; bound again (restart {count})", file=sys.stderr)

    forward = Forward(plan, directory, restarted)
    await forward.start()
    try:
        while not forward.bound():
            await asyncio.sleep(1)
        for port in plan.ports:
            print(f"{port.name}: {port.url() or f'127.0.0.1:{port.local_port}'}")
        await asyncio.Event().wait()
    finally:
        await forward.close()


def main(argv: list[str] | None = None, *, state_dir: Path | None = None) -> int:
    """Run ``clio-relay connect``."""
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
    base = args.state_dir or RelayPaths.from_env().state_dir
    ports: dict[str, int] = {}
    for item in args.port:
        name, _, value = str(item).partition("=")
        if not value.isdigit():
            parser.error(f"--port takes NAME=PORT, not {item!r}")
        ports[name] = int(value)
    try:
        loaded = serve_settings.load(serve_settings.settings_path(base))
        if loaded.rendezvous is None:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "set the rendezvous host on this machine first; see docs/rendezvous-host.md",
            )
        plan = visitor_plan(args.handle, loaded.rendezvous, ports)
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(hold(plan, base / "serve" / "connect" / plan.operation_id))
    except Refusal as refusal:
        print(f"clio-relay connect: {refusal}", file=sys.stderr)
        return 2
    return 0

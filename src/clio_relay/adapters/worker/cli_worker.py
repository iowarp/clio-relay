"""``clio-relay worker status``: registered worker capacity and leases.

``worker_status_command`` delegates to ``queue_management.worker_status`` and
``core_queue.ClioCoreQueue``. ``cli.py`` registers it on ``worker_app``.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from typing import Annotated

import typer

import clio_relay.adapters.worker.core_queue as core_queue
from clio_relay.config import RelaySettings
from clio_relay.adapters.worker.queue_management import worker_status

# `cli` is imported inside the command, not at module level, to avoid an
# import cycle (`cli.py` imports this module).


def worker_status_command(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
) -> None:
    """Show registered worker capacity and leases."""
    import clio_relay.adapters.worker.cli as cli

    args = ["status"]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    typer.echo(json.dumps(worker_status(queue, cluster=cluster), indent=2))

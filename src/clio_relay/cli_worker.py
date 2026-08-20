"""The ``worker`` command group (iowarp/clio-relay#231 cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)): the single
``worker_app`` command (registered worker capacity and leases) moves out of
the monolith into its own capped module, per ground rule 2 (SS2) --
``cli.py`` parses and renders only; this module does the same for its own
command and nothing more.

**Domain logic stays where it lives.** ``worker_status_command`` delegates
to ``queue_management.worker_status`` (imported directly -- not an audited
patch-seam collaborator) and ``core_queue.ClioCoreQueue`` (module-attribute
imported, since it *is* audited -- ``tests/test_cli_patch_seam.py``). It is
still used by many other groups remaining in ``cli.py``, so its ``caller``
entry stays ``"cli"``, same as ``cli_monitor.py``'s and ``cli_storage.py``'s
identical situation.

**What does NOT move here.** ``_try_remote_cluster_passthrough`` is a
cross-cutting ``cli.py`` helper used far beyond this group (29 call sites
across the file) -- moving its body here would just relocate SS2 ground
rule 2's violation, not fix it. It stays in ``cli.py`` and is reached
through the same import-cycle discipline ``cli_relay_host.py`` established.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from typing import Annotated

import typer

import clio_relay.core_queue as core_queue
from clio_relay.config import RelaySettings
from clio_relay.queue_management import worker_status

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
worker_app = typer.Typer(no_args_is_help=True)


@worker_app.command("status")
def worker_status_command(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
) -> None:
    """Show registered worker capacity and leases."""
    import clio_relay.cli as cli

    args = ["worker", "status"]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    typer.echo(json.dumps(worker_status(queue, cluster=cluster), indent=2))

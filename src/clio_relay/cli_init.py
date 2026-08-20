"""The ``init``/``install-frp`` top-level commands (iowarp/clio-relay#231
cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names the 13 flat, un-namespaced ``@app.command(...)`` entries directly on
``cli.py``'s top-level ``app`` as a group to split by concern. This module
owns the init/frp concern: ``init`` (local queue/spool/cluster-registry
bootstrap) and ``install-frp`` (download the local frp binaries) -- both
tiny, self-contained commands with no shared collaborators between them.

**Domain logic stays where it lives.** ``init`` delegates to
``storage_runtime.storage_managed_queue`` and ``ClusterRegistry.load``;
``install-frp`` delegates entirely to ``bootstrap.install_local_frp``. This
module's own code is parsing and result rendering only, ground rule 2.

**Registration seam.** Same as ``cli_diagnostics.py``: both commands attach
to the shared top-level ``app`` Typer instance cli.py owns, not a namespaced
sub-app of their own, so cli.py imports this module for its plain function
objects and applies the registration itself
(``app.command("init")(cli_init.init)``, etc.) rather than this module
decorating ``cli.app`` directly, which would recreate the import cycle its
own docstring (and ``cli_diagnostics.py``'s) explains.

**Collaborators.** ``storage_runtime.storage_managed_queue`` is an audited
patch-seam collaborator (``tests/test_cli_patch_seam.py``) but keeps its
``"cli"`` caller unchanged -- it has many other cli.py call sites (session
start/teardown, jarvis-mcp-validate, and others), so this module simply adds
a second, unregistered, module-attribute-style caller, the same shape
``cli_diagnostics.py`` established for ``remote_cli.should_execute_on_
cluster``. ``ClusterRegistry``/``default_registry_path`` and
``install_local_frp`` are not audited, so they are imported plainly, matching
``cli.py``'s own prior style for them. ``install-frp``'s only collaborator
beyond ``install_local_frp`` is ``_run_or_exit`` (``cli_support.py``'s
forwarder), reached through ``cli.py``'s own name via the established
function-local ``import clio_relay.cli as cli`` discipline; ``init`` uses no
such wrapper -- it never did in ``cli.py`` either, so this move preserves
that exactly.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

import clio_relay.storage_runtime as storage_runtime
from clio_relay.bootstrap import install_local_frp
from clio_relay.cluster_config import ClusterRegistry, default_registry_path
from clio_relay.config import RelaySettings

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring for the import-cycle discipline this supports.
# pyright: reportPrivateUsage=false


def init(
    migrate_legacy_output: Annotated[
        bool,
        typer.Option(
            help=(
                "Authorize migration of exact oversized v0.9 output events after every "
                "queue writer has been stopped and verified inactive."
            )
        ),
    ] = False,
) -> None:
    """Initialize local queue, spool, and cluster registry files."""
    settings = RelaySettings.from_env()
    storage_runtime.storage_managed_queue(settings, migrate_legacy_output=migrate_legacy_output)
    registry = ClusterRegistry.load(default_registry_path())
    typer.echo(
        f"initialized core={settings.core_dir} spool={settings.spool_dir} "
        f"clusters={','.join(sorted(registry.clusters))}"
    )


def install_frp(
    destination: Annotated[
        Path,
        typer.Option(help="Directory for frpc/frps binaries."),
    ] = Path(".tools/frp/bin"),
) -> None:
    """Download and install frp for the local desktop."""
    import clio_relay.cli as cli

    cli._run_or_exit(lambda: typer.echo(f"frpc={install_local_frp(destination)}"))

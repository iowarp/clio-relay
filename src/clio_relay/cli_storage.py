"""The ``storage`` command group (iowarp/clio-relay#231 cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)): the single
``storage_app`` command (machine-readable storage admission readiness)
moves out of the monolith into its own capped module, per ground rule 2
(SS2) -- ``cli.py`` parses and renders only; this module does the same for
its own command and nothing more.

**Domain logic stays where it lives.** ``storage_status`` delegates to
``storage_runtime.storage_managed_queue`` exactly as it did inside
``cli.py`` -- an already-correct owner module, imported module-attribute
style since it is one of R8(i)'s audited patch-seam collaborators
(``tests/test_cli_patch_seam.py``). It is still used by many other groups
remaining in ``cli.py`` (``init``, every ``gateway`` runtime command,
``queue retention-collect``, ``remote-mcp validate``), so its ``caller``
entry stays ``"cli"`` -- this module reaching it by module-attribute import
just gives this module a working call path too, same as
``cli_monitor.py``'s ``core_queue.ClioCoreQueue``.

This command has no other ``cli.py`` collaborator, so no
function-local ``import clio_relay.cli as cli`` is needed here at all.
"""

from __future__ import annotations

import json

import typer

import clio_relay.storage_runtime as storage_runtime
from clio_relay.config import RelaySettings

storage_app = typer.Typer(no_args_is_help=True)


@storage_app.command("status")
def storage_status() -> None:
    """Return machine-readable storage admission readiness."""
    queue = storage_runtime.storage_managed_queue(RelaySettings.from_env())
    typer.echo(json.dumps(queue.storage_runtime.status(), indent=2))

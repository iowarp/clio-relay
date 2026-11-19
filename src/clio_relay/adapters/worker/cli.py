"""The ``clio-relay worker`` command group.

It holds the one command the worker adapter needs on a site:
``site-serve``, which placement starts detached after its ssh pass
(:mod:`clio_relay.adapters.worker.site_server`). Setting up a site needs no
command: the person signs in once, and relay places the worker.
"""

from __future__ import annotations

import typer

from clio_relay.adapters.worker.site_server import site_serve_command

worker_app = typer.Typer(
    no_args_is_help=True,
    help="The worker relay places on a site. Placement starts it; nobody runs it by hand.",
)
worker_app.command("site-serve")(site_serve_command)


@worker_app.callback()
def main() -> None:
    """The worker relay places on a site."""

"""The hidden ``jarvis-runtime-authority`` command.

serve's JARVIS service runtime runs it on the cluster over ssh
(``remote_cli.run_remote_jarvis_runtime_authority``) to resolve one private
service authority. The logic lives in ``clio_relay.jarvis_service_runtime``;
this module only parses the options and prints the document.
"""

from __future__ import annotations

import json
from typing import Annotated

import typer

from clio_relay.config import RelaySettings


def jarvis_runtime_authority(
    execution_id: str,
    pipeline_id: Annotated[str, typer.Option(help="Exact JARVIS pipeline identity.")],
    package_id: Annotated[str, typer.Option(help="Exact JARVIS package identity.")],
    service_instance_id: Annotated[
        str,
        typer.Option(help="Exact JARVIS service instance identity."),
    ],
    revision: Annotated[int, typer.Option(help="Exact current service revision.")],
    token_sha256: Annotated[
        str,
        typer.Option(help="Public SHA-256 identity of the private bearer capability."),
    ],
) -> None:
    """Resolve one private JARVIS authority for relay-internal transport."""
    import clio_relay.adapters.worker.cli_support as cli_support
    from clio_relay.jarvis_service_runtime import (
        private_jarvis_service_runtime_authority_document,
        resolve_local_jarvis_service_runtime_authority,
    )

    def action() -> None:
        settings = RelaySettings.from_env()
        authority = resolve_local_jarvis_service_runtime_authority(
            jarvis_bin=settings.jarvis_bin,
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            package_id=package_id,
            service_instance_id=service_instance_id,
            revision=revision,
            token_sha256=token_sha256,
        )
        typer.echo(
            json.dumps(
                private_jarvis_service_runtime_authority_document(authority),
                sort_keys=True,
                separators=(",", ":"),
            )
        )

    cli_support._run_or_exit(action)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

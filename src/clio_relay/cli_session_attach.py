"""The ``session attach``/``session reconnect`` commands (iowarp/clio-relay#276, lane R-B).

Registers directly onto ``cli_session.session_app``, the same
side-effect-import pattern ``cli_session_start.py``/``cli_session_teardown.py``
establish (see ``cli_session.py``'s own docstring for the two-file-one-Typer
rationale). Both commands share one body: ``session reconnect`` is a thin,
discoverable alias for exactly the same explicit, user-authorized action
``session attach`` performs (iowarp/clio-relay#276 B2 design point 2) --
neither redials on its own; both surface the same typed refusals when the
identity cannot be resolved (:class:`~clio_relay.session_attach.
NoDurableSessionRecordError`) or the remote session cannot be verified live
(:class:`~clio_relay.session_attach.SessionNotAttachableError`).
"""

from __future__ import annotations

from typing import Annotated

import typer

import clio_relay.cli_session as cli_session
import clio_relay.session_attach as session_attach
from clio_relay.config import RelaySettings


def _run_attach(cluster: str) -> None:
    """Attach to (or explicitly reconnect) one cluster's owned-session channel.

    Holds the same ``owner_session_transition_lock`` (keyed on the resolved
    session id) that ``session start``/``session detach``/``session
    teardown`` already serialize against -- attach is otherwise the only
    session verb that could race a concurrent teardown into burning a real
    SSH dial and a 2FA prompt for a refusal that never had a session left to
    attach to (iowarp/clio-relay#276 review D4). Resolving the target BEFORE
    acquiring the lock (to learn which session to key it on) is a read-only
    lookup; ``attach_owned_session`` re-resolves it again, inside the lock,
    before doing anything that could dial.
    """
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    settings = RelaySettings.from_env()

    def action() -> None:
        target = session_attach.resolve_attach_target(cluster=cluster, settings=settings)
        with cli._session_transition_lock(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster=cluster,
            session_id=target.session_id,
        ):
            connection, resolved_target, channel_reestablished = (
                session_attach.attach_owned_session(
                    definition=definition,
                    settings=settings,
                )
            )
            report = session_attach.build_attach_report(
                connection=connection,
                target=resolved_target,
                channel_reestablished=channel_reestablished,
                definition=definition,
            )
            typer.echo(report.model_dump_json(indent=2))

    cli._run_or_exit(action)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


@cli_session.session_app.command("attach")
def session_attach_command(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
) -> None:
    """Attach to a cluster's owned session: verify ownership, resume the held
    channel in place (or perform the one authorized reconnect if it dropped),
    and list the session's running jobs."""
    _run_attach(cluster)


@cli_session.session_app.command("reconnect")
def session_reconnect_command(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
) -> None:
    """Alias for "session attach": the one explicit, user-authorized action
    that may replace a dropped owned-session channel. Never redials on its own."""
    _run_attach(cluster)

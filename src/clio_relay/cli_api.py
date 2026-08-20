"""The ``api`` command group (iowarp/clio-relay#231 cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)): the single
``api_app`` command (starting the desktop-facing HTTP API) moves out of the
monolith into its own capped module, per ground rule 2 (SS2) -- ``cli.py``
parses and renders only; this module does the same for its own command and
nothing more.

**Domain logic stays where it lives.** ``api_start`` delegates to
``session_lifecycle.publish_owned_session_api_startup_receipt`` and
``installation.verified_session_api_install_receipt`` (through
``_require_process_bound_session_api_release``, below) exactly as it did
inside ``cli.py`` -- both already-correct owner modules, imported
module-attribute style since both are audited patch-seam collaborators
(``tests/test_cli_patch_seam.py``). Unlike ``core_queue.ClioCoreQueue`` in
``cli_monitor.py`` or ``remote_cli.should_execute_on_cluster`` in
``cli_agent.py``, ``api_start`` was each symbol's *only* caller in
``cli.py`` (1 call site apiece) -- moving it here made cli.py's own copy of
both audited-collaborator call paths disappear, so this slice reassigns
their ``caller`` entry in ``AUDITED_COLLABORATORS`` from ``"cli"`` to
``"cli_api"`` and registers this module in ``_GUARDED_CALLERS``, the same
bookkeeping R8(ii) did for the three ``transport_probe`` entries when
``relay-host`` moved.

**What moves here as a private helper, and why.**
``_require_process_bound_session_api_release`` had exactly one call site in
the whole of ``cli.py`` -- ``api_start`` itself -- unlike the cross-cutting
helpers ``cli_monitor.py``/``cli_agent.py`` left behind (``_run_or_exit``,
``_require_cluster``, and friends, each with double-digit call sites across
unrelated groups). A single-caller private helper is domain logic for this
group, not shared plumbing, so it moves with its only caller rather than
staying in ``cli.py`` as a one-line delegation.

**The import-cycle discipline.** ``cli`` is never bound as a module-level
name here, matching ``cli_relay_host.py``/``cli_monitor.py``/
``cli_agent.py``: it is imported function-locally, as the first statement of
``api_start`` (``import clio_relay.cli as cli``, then
``cli.<symbol>(...)``) for the one remaining cross-cutting ``cli.py``
collaborator it still needs (none, as it happens -- ``api_start`` has no
cli.py-owned collaborator left once its private helper moves with it; the
import is omitted entirely rather than left dead).
"""

# `session_lifecycle`/`installation` module-attribute reads below are
# intentional: both are audited patch-seam collaborators
# (`tests/test_cli_patch_seam.py`), reassigned to this module as their sole
# caller (see this module's own docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import os
import re
from typing import Annotated

import typer
import uvicorn

import clio_relay.installation as installation
import clio_relay.session_lifecycle as session_lifecycle
from clio_relay.config import RelaySettings
from clio_relay.dev_mode import dev_mode_enabled
from clio_relay.errors import ConfigurationError
from clio_relay.session_lifecycle import SessionApiReleaseIdentity

api_app = typer.Typer(no_args_is_help=True)


def _require_process_bound_session_api_release() -> None:
    """Require the API process to match its release marker when one is present."""
    expected_sha256 = os.environ.get("CLIO_RELAY_API_RELEASE_IDENTITY_SHA256")
    if expected_sha256 is None:
        return
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise ConfigurationError("session API release identity marker is invalid")
    receipt = installation.verified_session_api_install_receipt()
    artifact_sha256 = receipt.artifact_sha256
    if artifact_sha256 is None:  # pragma: no cover - verified helper requires it
        if not dev_mode_enabled():
            raise ConfigurationError("session API installation identity is incomplete")
        artifact_sha256 = "0" * 64
    observed = SessionApiReleaseIdentity(
        distribution_version=receipt.distribution_version,
        artifact_sha256=artifact_sha256,
        software=receipt.software,
    )
    if observed.sha256() != expected_sha256:
        raise ConfigurationError("session API release identity does not match running package")


@api_app.command("start")
def api_start(
    host: Annotated[str, typer.Option(help="HTTP bind address.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="HTTP bind port.")] = 8765,
    require_token: Annotated[
        bool,
        typer.Option(help="Fail if CLIO_RELAY_API_TOKEN is not configured."),
    ] = False,
) -> None:
    """Start the desktop-facing HTTP API."""
    if require_token and RelaySettings.from_env().api_token is None:
        raise typer.BadParameter("CLIO_RELAY_API_TOKEN is required with --require-token")
    try:
        _require_process_bound_session_api_release()
    except ConfigurationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    # Import the process-bound app while its one-time gated environment is intact.
    # The startup receipt then scrubs the owner token from the environment, while
    # the app retains the validated settings needed to prove this session's identity.
    from clio_relay.http_api import app as relay_http_app

    session_lifecycle.publish_owned_session_api_startup_receipt()
    uvicorn.run(relay_http_app, host=host, port=port)

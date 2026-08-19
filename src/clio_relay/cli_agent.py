"""The ``agent`` command group (iowarp/clio-relay#231 cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)): the two
``agent_app`` commands (submitting a remote agent task, rendering the
desktop-side MCP profile) move out of the monolith into their own capped
module, per ground rule 2 (SS2) -- ``cli.py`` parses and renders only; this
module does the same for its own two commands and nothing more.

**Domain logic stays where it lives.** ``agent_render_mcp_config`` delegates
to ``mcp_server.render_agent_mcp_profile`` exactly as it did inside
``cli.py`` -- an already-correct owner module, imported directly since it is
not one of R8(i)'s audited patch-seam collaborators. ``agent_run`` delegates
to ``remote_cli.should_execute_on_cluster``, imported module-attribute style
because it *is* audited (``tests/test_cli_patch_seam.py``); it is still used
by many other groups remaining in ``cli.py``, so its ``caller`` entry stays
``"cli"`` -- this module reaching it by module-attribute import just gives
this module a working call path too, same as ``cli_monitor.py``'s
``core_queue.ClioCoreQueue``.

**What does NOT move here.** ``_require_cluster``, ``_run_remote_or_exit``,
``_submit_managed_job``, ``_artifact_use_refs``, ``_artifact_use_cli_value``,
and ``_artifact_use_idempotency_suffix`` are cross-cutting ``cli.py`` helpers
used far beyond this group (56, 17, 5, 11, 6, and 6 call sites respectively
across the file) -- moving their bodies here would just relocate SS2 ground
rule 2's violation, not fix it. ``_submit_managed_job`` is also the exact
call path ``tests/test_cli_patch_seam.py``'s
``test_sabotage_echo_storage_admission_error_via_cli{,_support}`` drives
through ``agent run`` (its own docstring names this command as "the
lightest real command that reaches ``_submit_managed_job``") -- relocating
``agent_run`` here does not change that call path's shape, since it still
reaches ``cli.py``'s ``_submit_managed_job``/``_managed_queue_from_env``/
``_echo_storage_admission_error`` through the same ``cli.<symbol>`` module
attribute lookup those sabotage tests patch.

**The import-cycle discipline.** Identical to ``cli_relay_host.py`` and
``cli_monitor.py``: ``cli`` is never bound as a module-level name here. It
is imported function-locally, as the first statement of each command body
that needs a ``cli.py`` collaborator (``import clio_relay.cli as cli``,
then ``cli.<symbol>(...)``). Never use a bare ``from clio_relay.cli import
<symbol>``, which would silently un-patch every test targeting the owner
and break the moment a future slice moves the symbol again (the coupling
``tests/test_cli_patch_seam.py`` polices, R8(i)).
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring) -- same discipline `cli_relay_host.py`/`cli_monitor.py` document
# for their own `pyright: reportPrivateUsage=false` pragma.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

import clio_relay.remote_cli as remote_cli
from clio_relay.config import RelaySettings
from clio_relay.mcp_server import render_agent_mcp_profile
from clio_relay.models import JobKind, RelayJob, RemoteAgentTaskSpec

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
agent_app = typer.Typer(no_args_is_help=True)


@agent_app.command("run")
def agent_run(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    prompt: Annotated[str, typer.Option(help="Prompt file path on the cluster.")],
    mcp_config: Annotated[
        str | None,
        typer.Option(help="Optional MCP config/profile path on the cluster."),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Dependency as ARTIFACT_ID=SHA256 or canonical JSON with provenance. Repeatable.",
        ),
    ] = None,
) -> None:
    """Submit a remote agent task on a configured cluster."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    artifact_uses = cli._artifact_use_refs(used_artifact)
    key = idempotency_key or (
        f"agent:{cluster}:{prompt}:{mcp_config}"
        + cli._artifact_use_idempotency_suffix(artifact_uses)
    )
    if remote_cli.should_execute_on_cluster(definition):
        args = [
            "agent",
            "run",
            "--cluster",
            cluster,
            "--prompt",
            prompt,
            "--idempotency-key",
            key,
        ]
        if mcp_config is not None:
            args.extend(["--mcp-config", mcp_config])
        for ref in cli._artifact_use_refs(used_artifact):
            args.extend(["--used-artifact", cli._artifact_use_cli_value(ref)])
        cli._run_remote_or_exit(definition, args)
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.REMOTE_AGENT,
        spec=RemoteAgentTaskSpec(prompt_path=prompt, mcp_config_path=mcp_config),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = cli._submit_managed_job(job)
    typer.echo(saved.job_id)


@agent_app.command("render-mcp-config")
def agent_render_mcp_config(
    output: Annotated[
        Path | None,
        typer.Option(help="Optional path to write the agent MCP profile TOML."),
    ] = None,
) -> None:
    """Render an agent profile that exposes the relay MCP tools."""
    rendered = render_agent_mcp_profile(settings=RelaySettings.from_env())
    if output is None:
        typer.echo(rendered)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    typer.echo(output)

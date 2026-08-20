"""The ``monitor`` command group (iowarp/clio-relay#231 cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)): the three
``monitor_app`` commands (regex-rule creation, listing, and evaluation over a
job's durable event stream) move out of the monolith into their own capped
module, per ground rule 2 (SS2) -- ``cli.py`` parses and renders only; this
module does the same for its own three commands and nothing more.

**Domain logic stays where it lives.** The commands below delegate to
``core_queue.ClioCoreQueue`` (the durable monitor-rule store) and
``relay_ops.evaluate_monitor_rules`` (rule evaluation) exactly as they did
inside ``cli.py`` -- both are already-correct owner modules, imported
module-attribute style for the same reason ``cli_relay_host.py`` imports
``transport_probe`` that way: ``core_queue.ClioCoreQueue`` is one of R8(i)'s
audited patch-seam collaborators (``tests/test_cli_patch_seam.py``), still
used by many other groups remaining in ``cli.py``, so its ``caller`` entry
stays ``"cli"`` -- this module reaching it by module-attribute import does
not change that assignment, it just gives this module a working call path
too.

**What does NOT move here.** ``_run_or_exit``, ``_json_object``, and
``_managed_queue_from_env`` are cross-cutting ``cli.py`` helpers used far
beyond this group (13, 16, and 13 call sites respectively across the file)
-- moving their bodies here would just relocate SS2 ground rule 2's
violation, not fix it. They stay in ``cli.py`` and are reached through the
same import-cycle discipline ``cli_relay_host.py`` established (see below).

**The import-cycle discipline.** Identical to ``cli_relay_host.py``: ``cli``
is never bound as a module-level name here. It is imported function-locally,
as the first statement of each command body that needs a ``cli.py``
collaborator (``import clio_relay.cli as cli``, then ``cli.<symbol>(...)``).
Never use a bare ``from clio_relay.cli import <symbol>``, which would
silently un-patch every test targeting the owner and break the moment a
future slice moves the symbol again (the coupling
``tests/test_cli_patch_seam.py`` polices, R8(i)).
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring) -- same discipline `cli_relay_host.py` documents for its own
# `pyright: reportPrivateUsage=false` pragma, one rule over.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from typing import Annotated

import typer

import clio_relay.core_queue as core_queue
from clio_relay.config import RelaySettings
from clio_relay.models import MonitorRule, MonitorRuleAction
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.relay_ops import evaluate_monitor_rules

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
monitor_app = typer.Typer(no_args_is_help=True)


@monitor_app.command("add-regex")
def monitor_add_regex(
    job_id: str,
    pattern: Annotated[str, typer.Option(help="Python regular expression to match.")],
    action: Annotated[
        MonitorRuleAction,
        typer.Option(help="Action to take when the rule matches."),
    ] = MonitorRuleAction.EMIT_EVENT,
    event_type: Annotated[
        list[str] | None,
        typer.Option(help="Event type to inspect; repeat for multiple types."),
    ] = None,
    action_payload_json: Annotated[
        str,
        typer.Option(help="JSON object used by actions such as submit_agent."),
    ] = "{}",
) -> None:
    """Create a generic regex monitor rule over a job event stream."""
    import clio_relay.cli as cli

    action_payload = cli._json_object(action_payload_json)
    rule = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir).append_monitor_rule(
        MonitorRule(
            job_id=job_id,
            pattern=pattern,
            action=action,
            event_types=event_type or [],
            action_payload=action_payload,
        )
    )
    typer.echo(rule.model_dump_json(indent=2))


@monitor_app.command("list")
def monitor_list(
    job_id: Annotated[
        str | None,
        typer.Option(help="Optional job id filter."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global monitor-rule source cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum monitor-rule source positions read.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable source window of durable monitor rules as JSON."""
    rules, next_cursor, total = core_queue.ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_monitor_rules_page(
        cursor=cursor,
        limit=limit,
        job_id=job_id,
    )
    typer.echo(
        json.dumps(
            {
                "rules": [rule.model_dump(mode="json") for rule in rules],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_monitor_rule_sequence_high_water",
                "filters_apply_within_source_window": True,
            },
            indent=2,
        )
    )


@monitor_app.command("run-once")
def monitor_run_once(
    limit: Annotated[int, typer.Option(help="Maximum events read per rule.")] = 100,
) -> None:
    """Evaluate enabled monitor rules once."""
    import clio_relay.cli as cli

    cli._run_or_exit(
        lambda: typer.echo(
            json.dumps(evaluate_monitor_rules(cli._managed_queue_from_env(), limit=limit), indent=2)
        )
    )

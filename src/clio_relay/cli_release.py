"""The ``release`` command group (iowarp/clio-relay#231 cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)): the three
``release_app`` commands (the complete local release gate, the release-gate
policy check, and clio-relay#198's fast preflight) move out of the monolith
into their own capped module, per ground rule 2 (SS2) -- ``cli.py`` parses
and renders only; this module does the same for its own three commands and
nothing more.

**Domain logic stays where it lives.** The commands below delegate to
``release_validation.run_local_release_validation``,
``validation_report_module.write_validation_report``, and
``release_pins.run_preflight``/``render_preflight`` exactly as they did
inside ``cli.py`` -- already-correct owner modules. Only
``run_local_release_validation`` and ``write_validation_report`` are audited
patch-seam collaborators (``tests/test_cli_patch_seam.py``), so only those
two are module-attribute imported; the rest (``load_release_gate_policy``,
``evaluate_release_gate``, ``write_release_gate_result``, ``run_preflight``,
``render_preflight``, ``LocalReleaseValidationOptions``) were each used
exclusively by this group in ``cli.py`` (a single call site apiece) and are
imported plainly, matching ``cli.py``'s own prior style for them.

**Reassigned patch-seam caller.** ``release_validation.
run_local_release_validation`` had exactly one call site in the whole of
``cli.py`` -- ``release_validate_local`` itself -- unlike
``validation_report_module.write_validation_report`` (24 call sites across
the file, stays ``"cli"``). This slice reassigns
``run_local_release_validation``'s ``caller`` entry in
``AUDITED_COLLABORATORS`` from ``"cli"`` to ``"cli_release"`` and registers
this module in ``_GUARDED_CALLERS``, the same bookkeeping R8(ii) did for the
three ``transport_probe`` entries when ``relay-host`` moved, and this
campaign already did for ``cli_api.py``'s two collaborators.

**What does NOT move here.** ``_load_current_acceptance_report`` is a
cross-cutting ``cli.py`` helper used beyond this group (5 call sites, 2 of
them here) -- moving its body here would just relocate SS2 ground rule 2's
violation, not fix it. ``_run_or_exit`` and ``_write_failed_acceptance_report``
are ``cli_support.py``'s cross-cutting helpers, reached the same way
``cli_relay_host.py`` reaches them. All three stay reachable through the
same import-cycle discipline: ``cli`` is never bound as a module-level name
here, only imported function-locally as the first statement of each command
body that needs one of these.

``_acceptance_report_command`` is applied as a bare decorator on
``release_validate_local`` -- read straight from ``cli_support`` at this
module's own import time, the same reason ``cli_relay_host.py``'s four
commands do (a decorator fires before any function-local import can run, so
routing it through ``cli.py`` would recreate the very import cycle the
function-local discipline exists to avoid).
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

import clio_relay.cli_support as cli_support
import clio_relay.release_validation as release_validation
import clio_relay.validation_report as validation_report_module
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.release_pins import render_preflight, run_preflight
from clio_relay.release_validation import (
    DEFAULT_CHECK_TIMEOUT_SECONDS,
    DEFAULT_PYTEST_PER_TEST_TIMEOUT_SECONDS,
    LocalReleaseValidationOptions,
)
from clio_relay.validation_report import (
    default_report_path,
    evaluate_release_gate,
    load_release_gate_policy,
    load_validation_report,
    new_live_validation_report,
    write_release_gate_result,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
release_app = typer.Typer(no_args_is_help=True)


@release_app.command("validate-local")
@cli_support._acceptance_report_command
def release_validate_local(
    project_root: Annotated[
        Path,
        typer.Option(help="Clean source checkout to validate."),
    ] = Path("."),
    report: Annotated[
        Path | None,
        typer.Option(help="JSON report path. Defaults under .clio-relay/validation-reports."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional human-readable Markdown rendering."),
    ] = None,
    artifact_dir: Annotated[
        Path | None,
        typer.Option(help="Optional empty output directory for wheel and sdist artifacts."),
    ] = None,
    prebuilt_artifact_dir: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Reuse an exact build-once wheel, sdist, and SHA256SUMS directory; "
                "never build artifacts in this validation run."
            )
        ),
    ] = None,
    check_timeout_seconds: Annotated[
        float,
        typer.Option(
            help=(
                "Overall wall-clock deadline for each check, in seconds. A wedged "
                "check's whole process tree is killed and the failure is recorded "
                "with a typed check_timeout reason instead of hanging silently "
                "(clio-relay#275)."
            )
        ),
    ] = DEFAULT_CHECK_TIMEOUT_SECONDS,
    pytest_per_test_timeout_seconds: Annotated[
        float,
        typer.Option(
            help=(
                "Per-test timeout, in seconds, for the local.pytest battery "
                "(pytest-timeout, thread method). On expiry the report names "
                "the hanging test instead of a bare nonzero exit."
            )
        ),
    ] = DEFAULT_PYTEST_PER_TEST_TIMEOUT_SECONDS,
) -> None:
    """Run the complete local release gate and persist evidence on failure."""
    import clio_relay.cli as cli

    report_path = report or default_report_path("local")
    seed_report = new_live_validation_report(
        scenario="local-release",
        cluster="local",
    )
    validation_report_module.write_validation_report(seed_report, report_path)

    def _run() -> None:
        try:
            result = release_validation.run_local_release_validation(
                LocalReleaseValidationOptions(
                    project_root=project_root,
                    report_path=report_path,
                    markdown_report_path=markdown_report,
                    artifact_dir=artifact_dir,
                    prebuilt_artifact_dir=prebuilt_artifact_dir,
                    report_id=seed_report.report_id,
                    check_timeout_seconds=check_timeout_seconds,
                    pytest_per_test_timeout_seconds=pytest_per_test_timeout_seconds,
                )
            )
            current_report = cli._load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            if current_report is None or result.report_id != seed_report.report_id:
                raise RelayError(
                    "local release validation did not persist the current invocation report"
                )
        except BaseException as exc:
            current_report = cli._load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            cli._write_failed_acceptance_report(
                path=report_path,
                scenario="local-release",
                cluster="local",
                check_id="local-release.completed",
                summary="complete local release gate",
                error=exc,
                launcher=None,
                install_source=None,
                artifact=None,
                partial_report=current_report or seed_report,
            )
            typer.echo(f"validation.report={report_path.resolve()}")
            raise
        typer.echo(f"validation.status={result.status.value}")
        typer.echo(f"validation.report={report_path.resolve()}")

    cli._run_or_exit(_run)


@release_app.command("gate")
def release_gate(
    policy: Annotated[Path, typer.Option(help="Machine-readable 1.0 release policy.")],
    report: Annotated[
        list[Path] | None,
        typer.Option(help="Validation JSON report. Repeat for multiple reports."),
    ] = None,
    report_dir: Annotated[
        Path | None,
        typer.Option(help="Directory containing validation JSON reports."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option(help="Optional JSON path for the gate decision."),
    ] = None,
    expected_artifact_sha256: Annotated[
        str | None,
        typer.Option(
            help=(
                "SHA-256 independently computed from the immutable candidate wheel. "
                "Every non-local report used by the gate must match it."
            )
        ),
    ] = None,
) -> None:
    """Reject a release unless every policy requirement has released-artifact evidence."""
    import clio_relay.cli as cli

    def _run() -> None:
        report_paths = list(report or [])
        if report_dir is not None:
            report_paths.extend(sorted(report_dir.glob("*.json")))
        unique_paths = list(dict.fromkeys(path.resolve() for path in report_paths))
        if not unique_paths:
            raise ConfigurationError("release gate requires --report or --report-dir")
        gate_policy = load_release_gate_policy(policy)
        reports = [load_validation_report(path) for path in unique_paths]
        result = evaluate_release_gate(
            gate_policy,
            reports,
            expected_artifact_sha256=expected_artifact_sha256,
        )
        if output is not None:
            write_release_gate_result(result, output)
        typer.echo(result.model_dump_json(indent=2))
        if not result.passed:
            raise typer.Exit(code=1)

    cli._run_or_exit(_run)


@release_app.command("preflight")
def release_preflight(
    project_root: Annotated[
        Path,
        typer.Option(help="Clean source checkout to check."),
    ] = Path("."),
) -> None:
    """Verify every release-identity pin agrees (clio-relay#198's fast local check)."""
    import clio_relay.cli as cli

    def _run() -> None:
        result = run_preflight(project_root)
        for line in render_preflight(result):
            typer.echo(line)
        if not result.passed:
            raise typer.Exit(code=1)

    cli._run_or_exit(_run)

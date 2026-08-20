"""The ``cluster`` deployment command group (iowarp/clio-relay#231).

``cli_cluster.py``'s own docstring named this exact scope as its deferred
second slice: the six commands that reach over SSH to bring a configured
cluster's runtime up -- ``probe``/``bootstrap``/``install-app``/
``install-endpoint-service``/``restart-endpoint-service``/
``endpoint-service-status`` -- as opposed to that module's four local
registry-CRUD commands (``list``/``add``/``pin-target``/``pin-runtime``).
This is a sibling file registering onto ``cli_cluster.py``'s ``cluster_app``
Typer instance (``@cli_cluster.cluster_app.command(...)``), the same
two-file-one-Typer pattern ``cli_queue.py``/``cli_queue_maintenance.py`` and
``cli_job.py``/``cli_job_records.py`` established -- the combined ten
commands span well past the 800-line new-file cap, so the module stays
split by this exact registry/deployment seam rather than merged into one.

**Two private helpers moved here.** ``_invalidate_remote_mcp_cache_after_
bootstrap`` had every one of its call sites inside this exact six-command
group (``cluster_bootstrap``) -- not shared plumbing, matching
``cli_cluster.py``'s own reasoning for ``_route_revision_before_edit``/
``_warn_if_route_revision_changed``/``_split_csv``. ``_resolved_worker_
capacity_policy`` (``cluster_install_endpoint_service``'s) turned out NOT
to be single-caller -- ``cli_endpoint.py``'s ``endpoint_start`` and
``endpoint_render_user_service`` also call it (a gap this fix-forward
closed: ``cli_endpoint.py`` now reaches it via ``import clio_relay.
cli_cluster_deploy as cli_cluster_deploy`` rather than the stale
``cli._resolved_worker_capacity_policy`` it was left calling when this
module was cut). It stays defined here rather than moving again to
``cli_support.py`` since this remains its primary/majority owner.

**Collaborators imported directly, not through ``cli``.** None of
``clio_relay.cluster_probe``, ``clio_relay.bootstrap_pin``,
``clio_relay.deployment``'s ``render_endpoint_user_service``,
``clio_relay.validation_report``'s reporting types, or
``clio_relay.errors``'s exceptions are audited patch-seam collaborators
(``tests/test_cli_patch_seam.py``) -- cli.py itself reached them by bare
import, so this module does too, matching cli.py's own prior style (the
same reasoning ``cli_cluster.py``'s own docstring documents for
``cluster_config.py``). ``clio_relay.bootstrap``, ``clio_relay.deployment``
(module-level), ``clio_relay.application_profiles``,
``clio_relay.endpoint_service_status``, and ``clio_relay.bootstrap_
acceptance`` ARE audited -- reached via ``import clio_relay.X as X`` then
``X.symbol(...)``, exactly as cli.py called them, so
``tests/test_cli_patch_seam.py``'s existing ``AUDITED_COLLABORATORS``
entries for those five just move their recorded caller from ``cli`` to
``cli_cluster_deploy`` rather than needing a new shape.
``RemoteMcpSchemaCache``/``default_remote_mcp_cache_path``
(``_invalidate_remote_mcp_cache_after_bootstrap``'s own collaborators) and
``_kind_concurrency_options`` (``_resolved_worker_capacity_policy``'s)
remain genuinely shared -- both still have call sites elsewhere in cli.py's
``remote-mcp``/other cluster-registry commands -- so they stay reached via
``cli.<symbol>`` through the function-local import below, unchanged.

**The import-cycle discipline.** ``cli`` is never bound as a module-level
name here, matching every prior extraction: it is imported function-locally,
as the first statement of each command body that needs a cross-cutting
``cli.py`` collaborator (``_require_cluster``, ``_run_or_exit``,
``_write_failed_acceptance_report``, ``_echo_lines``, ``_json_output``,
``_remote_target_identity``, ``RemoteMcpSchemaCache``,
``default_remote_mcp_cache_path``, ``_kind_concurrency_options``).
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring and `cli_cluster.py`'s identical discipline).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import hashlib
import json
import re
import time
from pathlib import Path
from typing import Annotated, cast

import typer
from pydantic import ValidationError

import clio_relay.application_profiles as application_profiles
import clio_relay.bootstrap as bootstrap
import clio_relay.bootstrap_acceptance as bootstrap_acceptance
import clio_relay.cli_cluster as cli_cluster
import clio_relay.cli_support as cli_support
import clio_relay.deployment as deployment
import clio_relay.endpoint_service_status as endpoint_service_status
from clio_relay.bootstrap_pin import pin_reconciliation_lines, reconcile_cluster_runtime_pin
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, WorkerCapacityPolicy
from clio_relay.cluster_probe import pinned_runtime_present, probe_cluster_runtime
from clio_relay.deployment import render_endpoint_user_service
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.validation_report import (
    EvidenceReference,
    ValidationRecorder,
    ValidationResource,
    default_report_path,
    new_live_validation_report,
)


def _invalidate_remote_mcp_cache_after_bootstrap(
    *,
    cluster: str,
    receipt: dict[str, object],
    registry_path: Path | None = None,
) -> dict[str, object]:
    """Invalidate cluster-scoped MCP schemas when bootstrap changes its generation."""
    import clio_relay.cli as cli

    raw_generation = receipt.get("generation")
    if not isinstance(raw_generation, dict):
        return {
            "schema_version": "clio-relay.remote-mcp-cache-invalidation.v1",
            "cluster": cluster,
            "previous_generation": None,
            "active_generation": None,
            "generation_changed": None,
            "action": "generation_unreported",
            "removed_server_count": 0,
            "removed_server_names": [],
        }
    generation = cast(dict[str, object], raw_generation)
    previous_generation = generation.get("previous")
    active_generation = generation.get("active")
    if previous_generation is not None and (
        not isinstance(previous_generation, str) or not previous_generation
    ):
        raise RelayError("bootstrap receipt has an invalid previous generation identity")
    if not isinstance(active_generation, str) or not active_generation:
        raise RelayError("bootstrap receipt has an invalid active generation identity")
    generation_changed = previous_generation != active_generation
    removed_server_names: tuple[str, ...] = ()
    if generation_changed:
        cache_path = cli.default_remote_mcp_cache_path(
            registry_path=registry_path or cli_cluster.default_registry_path()
        )
        _cache, removed_server_names = cli.RemoteMcpSchemaCache.invalidate_cluster_entries(
            cache_path,
            cluster,
        )
    return {
        "schema_version": "clio-relay.remote-mcp-cache-invalidation.v1",
        "cluster": cluster,
        "previous_generation": previous_generation,
        "active_generation": active_generation,
        "generation_changed": generation_changed,
        "action": "invalidated" if generation_changed else "preserved",
        "removed_server_count": len(removed_server_names),
        "removed_server_names": list(removed_server_names),
    }


def _resolved_worker_capacity_policy(
    definition: ClusterDefinition,
    *,
    concurrency: int | None,
    control_query_concurrency: int | None,
    kind_concurrency: list[str] | None,
    clear_kind_concurrency: bool,
) -> WorkerCapacityPolicy:
    """Resolve optional CLI overrides against one persisted worker policy."""
    import clio_relay.cli as cli

    if clear_kind_concurrency and kind_concurrency is not None:
        raise typer.BadParameter(
            "--clear-kind-concurrency cannot be combined with --kind-concurrency"
        )
    current = definition.worker_capacity
    selected_kind_concurrency = (
        {}
        if clear_kind_concurrency
        else (
            current.kind_concurrency
            if kind_concurrency is None
            else cli._kind_concurrency_options(kind_concurrency)
        )
    )
    try:
        return WorkerCapacityPolicy(
            concurrency=current.concurrency if concurrency is None else concurrency,
            control_query_concurrency=(
                current.control_query_concurrency
                if control_query_concurrency is None
                else control_query_concurrency
            ),
            kind_concurrency=selected_kind_concurrency,
        )
    except ValidationError as exc:
        raise typer.BadParameter(
            str(exc),
            param_hint="--concurrency/--control-query-concurrency",
        ) from exc


@cli_cluster.cluster_app.command("probe")
def cluster_probe_command(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
) -> None:
    """Report a cluster's runtime health without invoking the remote relay.

    Safe to run against a broken deployment: it never dereferences the pinned
    relay executable, so it still answers when everything else fails on it.
    """
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    cli._run_or_exit(lambda: typer.echo(json.dumps(probe_cluster_runtime(definition), indent=2)))


@cli_cluster.cluster_app.command("bootstrap")
@cli_support._acceptance_report_command
def cluster_bootstrap(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
    relay_wheel: Annotated[
        Path | None,
        typer.Option(
            "--relay-wheel",
            help="Local clio-relay wheel to include in the bootstrap archive.",
        ),
    ] = None,
    relay_artifact_sha256: Annotated[
        str | None,
        typer.Option(
            help=(
                "Expected lowercase SHA-256 of the exact clio-relay wheel. Required "
                "for release bootstrap, with or without --relay-wheel, so repeated "
                "offline bootstrap has an artifact-distinct identity."
            ),
        ),
    ] = None,
    report: Annotated[
        Path | None,
        typer.Option(help="Canonical cluster-bootstrap JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
) -> None:
    """Bootstrap a configured cluster's tools, relay package, and endpoint directories."""
    import clio_relay.cli as cli

    report_path = report or default_report_path(cluster)
    try:
        definition = cli._require_cluster(cluster)
    except BaseException as exc:
        cli._write_failed_acceptance_report(
            path=report_path,
            scenario="cluster-bootstrap",
            cluster=cluster,
            check_id="cluster.bootstrap.preflight",
            summary="validate cluster bootstrap acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=relay_wheel,
        )
        raise

    def action() -> None:
        action_started = time.monotonic()
        expected_artifact_sha256 = relay_artifact_sha256
        if expected_artifact_sha256 is not None and (
            re.fullmatch(r"[0-9a-f]{64}", expected_artifact_sha256) is None
        ):
            raise ConfigurationError("relay artifact SHA-256 must be lowercase hex")
        if relay_wheel is not None and expected_artifact_sha256 is None:
            raise ConfigurationError(
                "--relay-wheel requires --relay-artifact-sha256 so preflight never reads "
                "payload bytes before deciding whether transfer is needed"
            )
        validation = new_live_validation_report(
            scenario="cluster-bootstrap",
            cluster=cluster,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=expected_artifact_sha256,
        )
        recorder = ValidationRecorder(validation)
        try:
            with recorder.check(
                "cluster.bootstrap",
                "execute the real cluster bootstrap and retrieve its durable receipt",
            ) as evidence:
                lines = bootstrap.bootstrap_cluster_over_ssh(
                    bootstrap_profile=definition.bootstrap_profile,
                    ssh_host=ssh_host or definition.ssh_host,
                    source_root=bootstrap.package_source_root(),
                    cluster=definition.name,
                    core_dir=definition.core_dir,
                    spool_dir=definition.spool_dir,
                    relay_wheel=relay_wheel,
                    relay_artifact_sha256=expected_artifact_sha256,
                    agent_adapter=definition.agent_adapter,
                    agent_npm_package=definition.agent_npm_package,
                    agent_npm_bin=definition.agent_npm_bin,
                    agent_args=definition.agent_args,
                    jarvis_resource_graph_profile=(definition.jarvis_resource_graph_profile),
                    allow_jarvis_resource_graph_build=(
                        definition.allow_jarvis_resource_graph_build
                    ),
                )
                receipt_lines = [
                    line for line in lines if line.startswith("bootstrap_receipt_json=")
                ]
                if len(receipt_lines) != 1:
                    raise RelayError(
                        "bootstrap did not return exactly one durable invocation receipt"
                    )
                receipt_references = [
                    line.partition("=")[2]
                    for line in lines
                    if line.startswith("bootstrap_receipt=")
                ]
                if len(receipt_references) != 1 or not receipt_references[0]:
                    raise RelayError(
                        "bootstrap did not return exactly one durable receipt reference"
                    )
                receipt = cli._json_output(
                    receipt_lines[0].partition("=")[2],
                    "bootstrap invocation receipt",
                )
                cache_invalidation = _invalidate_remote_mcp_cache_after_bootstrap(
                    cluster=cluster,
                    receipt=receipt,
                )
                evidence.append(
                    EvidenceReference(
                        kind="remote_mcp_cache_invalidation",
                        reference=f"remote-mcp-cache:{cluster}",
                        metadata=cache_invalidation,
                    )
                )
                invocation_id = receipt.get("invocation_id")
                if not isinstance(invocation_id, str) or not invocation_id.startswith("bootstrap_"):
                    raise RelayError("bootstrap receipt omitted its unique invocation identity")
                evidence.append(
                    EvidenceReference(
                        kind="bootstrap_receipt",
                        reference=receipt_references[0],
                        metadata=receipt,
                    )
                )
                recorder.add_resource(
                    ValidationResource(
                        kind="bootstrap_invocation",
                        resource_id=invocation_id,
                        role="cluster_bootstrap",
                        cluster=cluster,
                        state="succeeded",
                        references=receipt_references,
                        metadata={
                            **receipt,
                            "ssh_host": ssh_host or definition.ssh_host,
                            "bootstrap_profile": definition.bootstrap_profile,
                            "output_sha256": hashlib.sha256("\n".join(lines).encode()).hexdigest(),
                            "remote_mcp_cache_invalidation": cache_invalidation,
                        },
                    )
                )
            with recorder.check(
                "worker.target-identity",
                "verify the bootstrapped physical cluster against the operator pin",
            ) as target_evidence:
                target_definition = (
                    definition.model_copy(update={"ssh_host": ssh_host})
                    if ssh_host is not None
                    else definition
                )
                target_identity = cli._remote_target_identity(target_definition)
                target_evidence.append(
                    EvidenceReference(
                        kind="cluster_target",
                        reference=f"ssh-target:{target_definition.ssh_host}",
                        metadata=target_identity,
                    )
                )
            recorder.add_resource(
                ValidationResource(
                    kind="cluster_target",
                    resource_id=f"target:{cluster}",
                    role="physical_cluster_target",
                    cluster=cluster,
                    state="verified",
                    provider=definition.scheduler_provider,
                    metadata=target_identity,
                )
            )
            with recorder.check(
                "cluster.bootstrap.runtime-pin",
                "repair the cluster registry pin only when it is proven broken",
            ) as pin_evidence:
                pin_reconciliation = reconcile_cluster_runtime_pin(
                    cluster=cluster,
                    registry_path=cli_cluster.default_registry_path(),
                    pinned_runtime_present=pinned_runtime_present(definition),
                )
                pin_evidence.append(
                    EvidenceReference(
                        kind="bootstrap_runtime_pin",
                        reference=f"cluster-pin:{cluster}",
                        metadata=pin_reconciliation,
                    )
                )
                lines.extend(pin_reconciliation_lines(pin_reconciliation))
            if receipt.get("outcome") in {"noop_verified", "repaired"}:
                with recorder.check(
                    "cluster.bootstrap.reuse-slo",
                    "enforce the bounded payload-free bootstrap reuse contract",
                ) as reuse_evidence:
                    reuse_acceptance = bootstrap_acceptance.bootstrap_reuse_acceptance_evidence(
                        receipt,
                        elapsed_seconds=time.monotonic() - action_started,
                    )
                    if reuse_acceptance is None:
                        raise RelayError(
                            "bootstrap reuse receipt did not produce acceptance evidence"
                        )
                    reuse_evidence.append(
                        EvidenceReference(
                            kind="bootstrap_reuse_acceptance",
                            reference=f"bootstrap-reuse:{invocation_id}",
                            metadata=reuse_acceptance,
                        )
                    )
        except BaseException as exc:
            recorder.finish(exc)
            recorder.write(report_path)
            raise
        recorder.finish()
        recorder.write(report_path)
        lines.append(f"validation.report={report_path.resolve()}")
        cli._echo_lines(lines)

    cli._run_or_exit(action)


@cli_cluster.cluster_app.command("install-app")
def cluster_install_app(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    app_name: Annotated[
        str,
        typer.Option("--app", help="Application runtime to install on the cluster."),
    ],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
) -> None:
    """Install an explicit application runtime on a configured cluster."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    cli._run_or_exit(
        lambda: cli._echo_lines(
            application_profiles.install_cluster_app_over_ssh(
                ssh_host=ssh_host or definition.ssh_host,
                app_name=app_name,
            )
        )
    )


@cli_cluster.cluster_app.command("install-endpoint-service")
def cluster_install_endpoint_service(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
    start: Annotated[bool, typer.Option(help="Restart the service after installing.")] = True,
    enable: Annotated[bool, typer.Option(help="Enable the user service.")] = True,
    concurrency: Annotated[
        int | None,
        typer.Option(
            help="Override and persist total worker slots; defaults to the cluster policy."
        ),
    ] = None,
    control_query_concurrency: Annotated[
        int | None,
        typer.Option(
            help=(
                "Override and persist slots reserved within total capacity for live "
                "control queries."
            )
        ),
    ] = None,
    kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--kind-concurrency",
            help="Per-kind worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
    clear_kind_concurrency: Annotated[
        bool,
        typer.Option(help="Clear and persist every per-kind worker capacity override."),
    ] = False,
    require_persistent: Annotated[
        bool,
        typer.Option(
            "--require-persistent/--allow-login-scoped",
            help=(
                "Require systemd user lingering so the enabled worker survives all logouts. "
                "The login-scoped opt-out is diagnostic and not release-gate eligible."
            ),
        ),
    ] = True,
) -> None:
    """Install a worker service from its persisted cluster capacity policy."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    capacity = _resolved_worker_capacity_policy(
        definition,
        concurrency=concurrency,
        control_query_concurrency=control_query_concurrency,
        kind_concurrency=kind_concurrency,
        clear_kind_concurrency=clear_kind_concurrency,
    )
    if capacity != definition.worker_capacity:
        ClusterRegistry.mutate(
            cli_cluster.default_registry_path(),
            lambda registry: registry.clusters.__setitem__(
                cluster,
                registry.require(cluster).model_copy(update={"worker_capacity": capacity}),
            ),
        )
        definition = definition.model_copy(update={"worker_capacity": capacity})
    service_text = render_endpoint_user_service(
        cluster=cluster,
        definition=definition,
        concurrency=capacity.concurrency,
        control_query_concurrency=capacity.control_query_concurrency,
        kind_concurrency=capacity.kind_concurrency,
    )
    cli._run_or_exit(
        lambda: cli._echo_lines(
            deployment.install_endpoint_user_service_over_ssh(
                cluster=cluster,
                ssh_host=ssh_host or definition.ssh_host,
                service_text=service_text,
                start=start,
                enable=enable,
                require_persistent=require_persistent,
            )
        )
    )


@cli_cluster.cluster_app.command("restart-endpoint-service")
def cluster_restart_endpoint_service(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
    require_persistent: Annotated[
        bool,
        typer.Option(
            "--require-persistent/--allow-login-scoped",
            help=(
                "Require systemd user lingering so the enabled worker survives all logouts. "
                "The login-scoped opt-out is diagnostic and not release-gate eligible."
            ),
        ),
    ] = True,
) -> None:
    """Verify persisted capacity, then restart without rewriting the installed unit."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    cli._run_or_exit(
        lambda: cli._echo_lines(
            deployment.restart_endpoint_user_service_over_ssh(
                cluster=cluster,
                ssh_host=ssh_host or definition.ssh_host,
                expected_capacity=definition.worker_capacity,
                require_persistent=require_persistent,
            )
        )
    )


@cli_cluster.cluster_app.command("endpoint-service-status")
def cluster_endpoint_service_status(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this read-only inspection."),
    ] = None,
) -> None:
    """Return machine-readable endpoint persistence and recovery readiness."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)

    def _status() -> None:
        evidence = endpoint_service_status.endpoint_service_readiness_over_ssh(
            cluster=cluster,
            ssh_host=ssh_host or definition.ssh_host,
        )
        typer.echo(evidence.model_dump_json(indent=2))

    cli._run_or_exit(_status)

"""The ``installation-write-receipt``/``installation-info``/
``bootstrap-inspect`` top-level commands (iowarp/clio-relay#231 cli.py
decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names the 13 flat, un-namespaced ``@app.command(...)`` entries directly on
``cli.py``'s top-level ``app`` as a group to split by concern. This module
owns the installation/receipt concern: minting a durable self-install
receipt, printing the current installation identity, and the bounded
payload-free bootstrap inspection/repair verb systemd bootstrap units use.

**Domain logic stays where it lives.** ``installation-write-receipt``
delegates to ``installation.write_self_install_receipt``; ``installation-
info`` delegates to ``installation.installation_info``; ``bootstrap-inspect``
composes ``bootstrap_reconcile``'s inspection primitives
(``proven_active_generation_mismatch``, ``inspect_exact_bootstrap_noop``,
``write_bootstrap_receipt``) with ``installation.worker_runtime_info``,
``core_queue.ClioCoreQueue``, and a bounded ``systemctl`` driver built on
``bounded_process.run_bounded_process`` -- all already-correct owners. This
module's own code is parsing and result rendering (plus the bootstrap-only
systemctl orchestration those owners don't do for it), ground rule 2.

**Registration seam.** Same as ``cli_diagnostics.py``/``cli_init.py``: all
three commands attach to the shared top-level ``app`` Typer instance cli.py
owns, not a namespaced sub-app of their own, so cli.py imports this module
for its plain function objects and applies the registration itself
(``app.command("installation-write-receipt")(cli_installation_receipt.
installation_write_receipt)``, etc.).

**Exclusive constants and imports moved with their only caller.**
``BOOTSTRAP_EXACT_INSPECTION_DEADLINE_SECONDS``/``BOOTSTRAP_REPAIR_
DEADLINE_SECONDS`` and the ``BootstrapDesiredState``/``make_bootstrap_
receipt`` imports had exactly one caller in cli.py -- ``bootstrap_inspect``
itself -- so they move here outright, no forwarder needed.

**Reassigned patch-seam callers.** Six audited collaborators had exactly one
call site in the whole of cli.py -- inside the three commands moving here,
and no *other* reference (call or type annotation) left behind -- unlike
``installation.installation_info`` and ``core_queue.ClioCoreQueue`` (both
still genuinely reached from cli.py: ``installation_info`` by session-
teardown machinery that stays cli.py-resident, and ``ClioCoreQueue`` as a
parameter type annotation across dozens of that same machinery's functions,
even though ``bootstrap-inspect`` was its only *constructor call* -- the
patch-seam guard cares about any module-attribute reference remaining, not
just calls, so both keep their ``"cli"`` caller entry unchanged). This slice
reassigns the other six's ``caller`` entry in ``AUDITED_COLLABORATORS`` from
``"cli"`` to ``"cli_installation_receipt"`` and registers this module in
``_GUARDED_CALLERS``: ``installation.write_self_install_receipt``,
``bootstrap_reconcile.proven_active_generation_mismatch``,
``bounded_process.run_bounded_process``,
``installation.worker_runtime_info``,
``bootstrap_reconcile.write_bootstrap_receipt``, and
``bootstrap_reconcile.bootstrap_invocation_lock`` (the ``_inspect`` wrapper's
serialization lock, held around ``_inspect_locked``) -- the last of these,
``bootstrap_reconcile`` itself, and ``bounded_process``/
``BoundedProcessError`` are now dead in cli.py entirely (ruff F401 confirmed
zero remaining references), so all three module imports were removed there.

**Collaborator reached through cli.py's own name (not moved here).**
``_run_or_exit`` (``cli_support.py``'s forwarder, ``installation-write-
receipt``'s only wrapper) is reached through cli.py's own name via the
established function-local ``import clio_relay.cli as cli`` discipline;
``installation-info`` reaches it the same way. ``bootstrap-inspect`` never
used ``_run_or_exit`` in cli.py either -- it raises directly, preserved
exactly.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal

import typer
from pydantic import ValidationError

import clio_relay.bootstrap_reconcile as bootstrap_reconcile
import clio_relay.bounded_process as bounded_process
import clio_relay.core_queue as core_queue
import clio_relay.installation as installation
from clio_relay.bootstrap_reconcile import BootstrapDesiredState, make_bootstrap_receipt
from clio_relay.bounded_process import BoundedProcessError
from clio_relay.config import RelaySettings
from clio_relay.dev_mode import VerificationFindings, dev_mode_enabled
from clio_relay.errors import ConfigurationError, RelayError

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring for the import-cycle discipline this supports.
# pyright: reportPrivateUsage=false

BOOTSTRAP_EXACT_INSPECTION_DEADLINE_SECONDS = 24.0
BOOTSTRAP_REPAIR_DEADLINE_SECONDS = 55.0


def installation_write_receipt(
    output: Annotated[Path, typer.Option(help="Destination install-receipt.json path.")],
    self_flag: Annotated[
        bool,
        typer.Option(
            "--self",
            help="Describe this process's own running installation (currently the only mode).",
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(help="Overwrite an existing receipt already at the destination path."),
    ] = False,
    components_from: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Copy components/component_artifacts verbatim from this generation "
                "receipt, for a mixed install where relay is self-described but "
                "components (clio-kit, jarvis-cd, ...) still come from a bootstrap "
                "generation's locked runtime."
            ),
        ),
    ] = None,
    dev_mode: Annotated[
        bool,
        typer.Option(
            help=(
                "clio-relay#211: mint a best-effort receipt even when identity "
                "derivation would otherwise fail, recording each finding as a "
                "warning instead. Never for production."
            ),
        ),
    ] = False,
) -> None:
    """Mint a durable install receipt describing this process's own running identity.

    A cluster's pinned runtime (``cluster pin-runtime --install-receipt``,
    clio-relay#205) points at a receipt written this way. Identity is
    resolved exactly as the persistent-uv-tool identity check already
    trusts it: a wheel's sha256 for a WHEEL/PYPI install, or the exact
    pinned commit sha for an exact-sha VCS install (clio-relay#206).

    ``--components-from`` supports a legitimate mixed dev-channel install:
    relay identity is minted for this process (self), while
    components/component_artifacts are inherited verbatim from another
    receipt -- the generation that genuinely installed them -- with the
    source path recorded on the minted receipt for provenance.
    """
    import clio_relay.cli as cli

    if not self_flag:
        raise typer.BadParameter("--self is required; only self-description is supported")
    resolved_dev_mode = dev_mode_enabled(cluster_dev_mode=dev_mode)
    findings = VerificationFindings()

    def action() -> None:
        receipt = installation.write_self_install_receipt(
            output,
            force=force,
            components_from=components_from,
            dev_mode=resolved_dev_mode,
            findings=findings,
        )
        payload: dict[str, object] = receipt.model_dump(mode="json")
        dev_mode_payload = findings.payload()
        if dev_mode_payload is not None:
            payload.update(dev_mode_payload)
        typer.echo(json.dumps(payload, indent=2, default=str))

    cli._run_or_exit(action)


def show_installation_info() -> None:
    """Print the current package identity and durable cluster install receipt."""
    import clio_relay.cli as cli

    cli._run_or_exit(
        lambda: typer.echo(json.dumps(installation.installation_info(), indent=2, default=str))
    )


def bootstrap_inspect(
    invocation_id: Annotated[
        str,
        typer.Option(help="Unique bootstrap invocation identity."),
    ],
    repair: Annotated[
        bool,
        typer.Option(
            "--repair/--inspect-only",
            help="Apply only the typed payload-free repair returned by an inspect-only call.",
        ),
    ] = False,
) -> None:
    """Perform a bounded payload-free inspection or explicit typed repair."""

    def _inspect_locked() -> None:
        encoded = os.environ.get("CLIO_RELAY_BOOTSTRAP_DESIRED_STATE_BASE64", "")
        if not encoded or len(encoded) > 128 * 1024:
            raise ConfigurationError("bootstrap desired state environment is missing or oversized")
        if re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", invocation_id) is None:
            raise ConfigurationError("bootstrap invocation identity is invalid")
        try:
            raw = base64.b64decode(encoded, validate=True)
            if len(raw) > 64 * 1024:
                raise ConfigurationError("bootstrap desired state exceeds its decoded bound")
            desired = BootstrapDesiredState.model_validate_json(raw)
        except (binascii.Error, UnicodeError, ValidationError, ValueError) as exc:
            raise ConfigurationError("bootstrap desired state is invalid") from exc
        active_generation = bootstrap_reconcile.proven_active_generation_mismatch(desired)
        if active_generation is not None:
            payload: dict[str, object] = {
                "schema_version": "clio-relay.bootstrap-preflight.v1",
                "exact_match": False,
                "desired_fingerprint": desired.fingerprint,
                "reasons": [
                    "active generation differs from desired fingerprint",
                ],
                "receipt": None,
                "action": "payload_required",
            }
            typer.echo(
                "bootstrap_preflight_json="
                + json.dumps(payload, sort_keys=True, separators=(",", ":"))
            )
            return
        started_at = datetime.now(UTC)
        started = time.monotonic()
        deadline = started + (
            BOOTSTRAP_REPAIR_DEADLINE_SECONDS
            if repair
            else BOOTSTRAP_EXACT_INSPECTION_DEADLINE_SECONDS
        )

        def run_systemctl(
            arguments: list[str], *, timeout_seconds: float
        ) -> subprocess.CompletedProcess[str]:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ConfigurationError("bootstrap inspection exceeded its total deadline")
            try:
                return bounded_process.run_bounded_process(
                    ["systemctl", "--user", *arguments],
                    timeout_seconds=min(timeout_seconds, remaining),
                    stdout_maximum_bytes=4096,
                    stderr_maximum_bytes=4096,
                )
            except (OSError, BoundedProcessError) as exc:
                raise ConfigurationError(
                    f"bounded systemd inspection failed: {arguments[0]}"
                ) from exc

        inspection_started = time.monotonic()
        current_installation = installation.installation_info()
        service_active: bool | None = None
        service_enabled: bool | None = None
        if desired.worker_service is not None:
            active_result = run_systemctl(
                ["is-active", "--quiet", desired.worker_service],
                timeout_seconds=5,
            )
            enabled_result = run_systemctl(
                ["is-enabled", "--quiet", desired.worker_service],
                timeout_seconds=5,
            )
            service_active = active_result.returncode == 0
            service_enabled = enabled_result.returncode == 0
        queue_evidence = core_queue.ClioCoreQueue(
            RelaySettings.from_env().core_dir
        ).readiness_info()
        worker_evidence: dict[str, object] | None = None
        if service_active is True and desired.cluster is not None:
            try:
                worker_evidence = installation.worker_runtime_info(
                    cluster=desired.cluster,
                    current_installation=current_installation,
                )
            except (RelayError, ValueError) as exc:
                worker_evidence = {
                    "schema_version": "clio-relay.worker-runtime-info.v1",
                    "cluster": desired.cluster,
                    "running": False,
                    "error": str(exc),
                }
        inspection = bootstrap_reconcile.inspect_exact_bootstrap_noop(
            desired,
            service_was_active=service_active,
            service_was_enabled=service_enabled,
            queue_evidence=queue_evidence,
            worker_evidence=worker_evidence,
            installation_snapshot=current_installation,
        )
        initial_service_active = service_active
        initial_service_enabled = service_enabled
        initial_inspection_reasons = list(inspection.reasons)
        initial_jarvis_state = inspection.jarvis_state
        service_start_count = 0
        service_enable_count = 0
        service_restart_count = 0
        repair_attempted = False
        repairable_reasons = {
            "managed endpoint service is inactive",
            "managed endpoint service is disabled",
            "active endpoint worker readiness did not verify",
        }
        if (
            repair
            and desired.worker_service is not None
            and inspection.reasons
            and set(inspection.reasons).issubset(repairable_reasons)
        ):
            repair_attempted = True
            load_state = run_systemctl(
                [
                    "show",
                    "--property=LoadState",
                    "--value",
                    desired.worker_service,
                ],
                timeout_seconds=5,
            )
            if not (
                load_state.returncode == 0
                and len(load_state.stdout.encode()) <= 1024
                and load_state.stdout.strip() == "loaded"
            ):
                raise ConfigurationError(
                    "managed endpoint service is not installed; run "
                    "cluster install-endpoint-service before requesting readiness repair"
                )
            else:
                if service_enabled is not True:
                    enabled = run_systemctl(
                        ["enable", desired.worker_service],
                        timeout_seconds=15,
                    )
                    if enabled.returncode != 0:
                        raise ConfigurationError("managed endpoint service could not be enabled")
                    service_enable_count = 1
                if service_active is True:
                    started_service = run_systemctl(
                        ["restart", desired.worker_service],
                        timeout_seconds=20,
                    )
                    if started_service.returncode != 0:
                        raise ConfigurationError("managed endpoint service could not be restarted")
                    service_restart_count = 1
                else:
                    started_service = run_systemctl(
                        ["start", desired.worker_service],
                        timeout_seconds=20,
                    )
                    if started_service.returncode != 0:
                        raise ConfigurationError("managed endpoint service could not be started")
                    service_start_count = 1
                worker_deadline = min(deadline, time.monotonic() + 30)
                worker_evidence = None
                while time.monotonic() < worker_deadline:
                    try:
                        worker_evidence = installation.worker_runtime_info(
                            cluster=desired.cluster or "",
                            current_installation=current_installation,
                        )
                    except (RelayError, ValueError):
                        time.sleep(0.25)
                        continue
                    if worker_evidence.get("running") is True:
                        break
                    time.sleep(0.25)
                service_active = True
                service_enabled = True
                inspection = bootstrap_reconcile.inspect_exact_bootstrap_noop(
                    desired,
                    service_was_active=True,
                    service_was_enabled=True,
                    queue_evidence=queue_evidence,
                    worker_evidence=worker_evidence,
                    installation_snapshot=current_installation,
                )
        if repair_attempted and not inspection.exact_match:
            raise ConfigurationError(
                "payload-free bootstrap repair did not converge: " + "; ".join(inspection.reasons)
            )
        payload: dict[str, object] = {
            "schema_version": "clio-relay.bootstrap-preflight.v1",
            "exact_match": inspection.exact_match,
            "desired_fingerprint": desired.fingerprint,
            "reasons": inspection.reasons,
            "receipt": None,
        }
        if inspection.exact_match:
            inspection_duration = time.monotonic() - inspection_started
            outcome: Literal["noop_verified", "repaired"] = (
                "repaired"
                if service_start_count or service_enable_count or service_restart_count
                else "noop_verified"
            )
            receipt = make_bootstrap_receipt(
                invocation_id=invocation_id,
                desired=desired,
                outcome=outcome,
                inspection=inspection,
                started_at=started_at,
                transaction=None,
                previous_generation=inspection.active_generation,
                active_generation=inspection.active_generation,
                duration_seconds=time.monotonic() - started,
                inspection_duration_seconds=inspection_duration,
                service_start_count=service_start_count,
                service_enable_count=service_enable_count,
                service_restart_count=service_restart_count,
                initial_inspection_reasons=initial_inspection_reasons,
                jarvis_state_before=initial_jarvis_state,
                service_active_before=initial_service_active,
                service_enabled_before=initial_service_enabled,
                service_active_after=service_active,
                service_enabled_after=service_enabled,
            )
            bootstrap_reconcile.write_bootstrap_receipt(
                Path.home() / ".local/share/clio-relay/bootstrap-receipt.json",
                receipt,
            )
            payload["receipt"] = receipt
            payload["action"] = outcome
        else:
            repairable = bool(inspection.reasons) and all(
                reason in repairable_reasons for reason in inspection.reasons
            )
            payload["action"] = (
                "repair_required" if not repair and repairable else "payload_required"
            )
            if payload["action"] == "repair_required":
                payload["repair_reasons"] = inspection.reasons
        typer.echo(
            "bootstrap_preflight_json=" + json.dumps(payload, sort_keys=True, separators=(",", ":"))
        )

    def _inspect() -> None:
        with bootstrap_reconcile.bootstrap_invocation_lock(timeout_seconds=2):
            _inspect_locked()

    import clio_relay.cli as cli

    cli._run_or_exit(_inspect)

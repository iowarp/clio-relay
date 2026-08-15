"""Shared CLI plumbing extracted off ``cli.py`` (iowarp/clio-relay#231, R8+).

``docs/design/relay-architecture-2026-08.md`` §4.1 names six cross-cutting
helpers used by most of ``cli.py``'s 16 Typer sub-apps, not any single
command group: ``_run_or_exit`` (74 call sites), ``_require_cluster`` (56),
``_write_failed_acceptance_report`` (19), ``_resolve_env_secret`` (19),
``_acceptance_report_command`` (17 applications), and ``default_report_path``
(18, already owned by ``validation_report.py`` -- not duplicated here). §5's
target-owner-module map assigns them to a new ``cli_support.py`` rather than
letting the first command-module extraction (R8(ii), the ``relay-host``
group) invent its own copy or reach backward into ``cli.py`` for logic that
has no other true owner.

Two private helpers move alongside the symbol that is their only caller:
``_local_secret`` (``_resolve_env_secret``'s `.clio-relay/secrets.json`
fallback) and ``_echo_storage_admission_error`` (the storage-refusal envelope
``_run_or_exit`` renders on a caught ``StorageAdmissionError``, also called
directly by two unrelated ``cli.py`` command bodies).

``cli.py`` keeps each of these names bound at their original module-level
definition sites as a one-line re-export (``_run_or_exit =
cli_support._run_or_exit``, ...) rather than rewriting its ~200 existing
bare-name call sites and the ~10 existing ``monkeypatch.setattr(cli, "_X",
...)`` test patches across this and other command groups in the same slice
that extracts one group's commands -- that rewrite is real future work
(every other command group migrating onto ``cli_support.X(...)`` directly),
not something the ``relay-host`` extraction should absorb as a side effect.
The new ``cli_relay_host.py`` module reaches these through ``cli.py``'s
existing re-export (``import clio_relay.cli as cli``, then
``cli._require_cluster(...)``), which is why the alias must keep the same
name cli.py always exposed it under.
"""

# The five collaborator entry points below (`_acceptance_report_command`,
# `_run_or_exit`, `_require_cluster`, `_write_failed_acceptance_report`,
# `_resolve_env_secret`) are called only through cli.py's re-export -- see
# this module's own docstring -- which pyright cannot trace as a real
# reference from inside this file. `http_api.py` sets the same
# `reportUnusedFunction=false` for its own decorator-registered-only route
# handlers; this is the same "the real caller is invisible from here" shape.
# pyright: reportUnusedFunction=false

from __future__ import annotations

import json
import os
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import typer
from pydantic import ValidationError

from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, default_registry_path
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.storage_runtime import StorageAdmissionError
from clio_relay.validation_report import (
    LiveValidationReport,
    ValidationRecorder,
    ValidationStatus,
    load_validation_report,
    new_live_validation_report,
    sha256_file,
)

_ACCEPTANCE_REPORT_COMMAND_ATTRIBUTE = "__clio_relay_acceptance_report_command__"


def _acceptance_report_command[CommandCallback: Callable[..., Any]](
    callback: CommandCallback,
) -> CommandCallback:
    """Mark a CLI callback as a canonical acceptance-report producer."""
    setattr(callback, _ACCEPTANCE_REPORT_COMMAND_ATTRIBUTE, True)
    return callback


def _echo_storage_admission_error(error: StorageAdmissionError) -> None:
    """Write the stable CLI storage refusal envelope to stderr."""
    typer.echo(
        json.dumps(
            {
                "error": "storage_admission_denied",
                "storage_decision": error.decision.to_dict(),
            },
            sort_keys=True,
        ),
        err=True,
    )


def _run_or_exit(action: Callable[[], None]) -> None:
    try:
        action()
    except StorageAdmissionError as exc:
        _echo_storage_admission_error(exc)
        raise typer.Exit(code=1) from exc
    except (ConfigurationError, RelayError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _require_cluster(cluster: str) -> ClusterDefinition:
    return ClusterRegistry.load(default_registry_path()).require(cluster)


def _write_failed_acceptance_report(
    *,
    path: Path,
    scenario: str,
    cluster: str,
    check_id: str,
    summary: str,
    error: BaseException,
    launcher: str | None,
    install_source: str | None,
    artifact: Path | None,
    partial_report: LiveValidationReport | None = None,
) -> None:
    """Persist one canonical failed report without discarding partial evidence."""
    report = partial_report
    if partial_report is not None and path.exists():
        with suppress(OSError, ValidationError, ValueError):
            existing = load_validation_report(path)
            if existing.report_id == partial_report.report_id:
                expected_error = f"{type(error).__name__}: {error}"
                already_recorded = (
                    existing.status is ValidationStatus.FAILED
                    and existing.error == expected_error
                    and any(
                        check.check_id == check_id
                        and check.status is ValidationStatus.FAILED
                        and check.error == expected_error
                        for check in existing.checks
                    )
                )
                if already_recorded:
                    return
                # The caller's in-memory report may contain the latest observation that
                # failed before its next checkpoint write. The on-disk copy is used only
                # for idempotency here; replacing the partial would discard that evidence.
    artifact_sha256: str | None = None
    if artifact is not None:
        with suppress(OSError):
            artifact_sha256 = sha256_file(artifact)
    if report is None:
        report = new_live_validation_report(
            scenario=scenario,
            cluster=cluster,
            launcher=launcher,
            install_source=install_source,
            artifact_sha256=artifact_sha256,
        )
    recorder = ValidationRecorder(report)
    recorder.record_failure(check_id, summary, error)
    recorder.finish(error)
    recorder.write(path)


def _local_secret(env_name: str) -> str | None:
    path = Path(".clio-relay/secrets.json")
    if not path.exists():
        return None
    loaded = cast(object, json.loads(path.read_text(encoding="utf-8-sig")))
    if not isinstance(loaded, dict):
        raise ConfigurationError(".clio-relay/secrets.json must contain a JSON object")
    secrets = cast(dict[object, object], loaded)
    value = secrets.get(env_name)
    if value is None:
        return None
    if not isinstance(value, str) or value == "":
        raise ConfigurationError(
            f".clio-relay/secrets.json field must be a non-empty string: {env_name}"
        )
    return value


def _resolve_env_secret(value: str | None, env_name: str, label: str) -> str:
    resolved = value or os.getenv(env_name) or _local_secret(env_name)
    if resolved:
        return resolved
    raise ConfigurationError(
        f"{label} is required; pass it explicitly, set {env_name}, "
        f"or add {env_name} to .clio-relay/secrets.json"
    )

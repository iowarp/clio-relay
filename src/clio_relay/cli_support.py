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

**#231 cli.py decomposition, shared-plumbing relocation pass.** SS5's
target-owner-map row for this module also names ``_json_object``,
``_managed_queue_from_env``, and the artifact-use helpers -- left in cli.py
through R8(ii) because cli.py's own top-level command bodies (``mcp-call``,
``jarvis-mcp-call``, ``jarvis-mcp-refresh``, ``jarvis-mcp-validate``) were
still their heaviest resident callers at the time, the same "cli.py is
still the primary user" argument R8(ii) used for the five helpers above.
The jarvis-mcp command-group extraction moved every one of those bodies out
of cli.py, so the argument no longer holds: ``_submit_managed_job``,
``_artifact_use_refs``, and ``_artifact_use_cli_value`` now have zero
remaining cli.py-*internal* callers (only the external ``cli.<symbol>``
call sites the already-extracted ``cli_agent.py``/``cli_job.py``/
``cli_job_records.py``/``cli_gateway.py``/``cli_monitor.py``/``cli_queue.py``/
``cli_queue_maintenance.py`` reach through cli.py's forwarder), while
``_managed_queue_from_env``, ``_json_object``, ``_json_text_from_option``,
and ``_environment_references`` still have one or two -- session start/
teardown for the queue opener, the still-unsequenced remote-mcp validation
command for the JSON parsers and the env-from parser. Both cases get the
identical treatment already established above: real body here, thin
forwarder in cli.py under the original name, so neither cli.py's own
remaining callers nor any external module's existing ``cli.<symbol>`` call
site needs to change.
"""

# The five collaborator entry points below (`_acceptance_report_command`,
# `_run_or_exit`, `_require_cluster`, `_write_failed_acceptance_report`,
# `_resolve_env_secret`) are called only through cli.py's re-export -- see
# this module's own docstring -- which pyright cannot trace as a real
# reference from inside this file. `http_api.py` sets the same
# `reportUnusedFunction=false` for its own decorator-registered-only route
# handlers; this is the same "the real caller is invisible from here" shape.
# Scope (F6, iowarp/clio-relay#231 R8(ii) review): covers only this module's
# re-exported entry points (called through cli.py's forwarders/re-exports
# and cli_relay_host.py's direct `cli_support.<symbol>` reads).
# pyright: reportUnusedFunction=false

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from contextlib import suppress
from json import JSONDecodeError
from pathlib import Path
from typing import Any, cast

import typer
from pydantic import ValidationError

import clio_relay.storage_runtime as storage_runtime
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, default_registry_path
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    ArtifactUse,
    RelayJob,
    artifact_use_payload,
    validate_artifact_use_collection,
)
from clio_relay.storage_runtime import StorageAdmissionError, StorageManagedQueue
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


def _managed_queue_from_env() -> StorageManagedQueue:
    """Open the production queue with durable storage reconciliation enabled."""
    return storage_runtime.storage_managed_queue(RelaySettings.from_env())


def _submit_managed_job(job: RelayJob) -> RelayJob:
    """Submit through storage admission and emit stable JSON on refusal."""
    try:
        return _managed_queue_from_env().submit_job(job)
    except StorageAdmissionError as exc:
        _echo_storage_admission_error(exc)
        raise typer.Exit(code=1) from exc


def _json_object(value: str) -> dict[str, object]:
    source = Path(value[1:]).read_text(encoding="utf-8-sig") if value.startswith("@") else value
    try:
        loaded = cast(object, json.loads(source))
    except JSONDecodeError as exc:
        raise typer.BadParameter(f"value must be valid JSON: {exc.msg}") from exc
    if not isinstance(loaded, dict):
        raise typer.BadParameter("value must be a JSON object")
    return {str(key): item for key, item in cast(dict[object, object], loaded).items()}


def _json_text_from_option(source: str, source_file: Path | None) -> str:
    if source_file is None:
        return source
    if source != "{}":
        raise typer.BadParameter("use either the JSON value option or the JSON file option")
    if not source_file.exists():
        raise typer.BadParameter(f"JSON file does not exist: {source_file}")
    return source_file.read_text(encoding="utf-8-sig")


def _environment_references(items: list[str] | None) -> dict[str, str]:
    """Parse repeatable CHILD=SOURCE environment references without reading values."""
    references: dict[str, str] = {}
    for item in items or []:
        child_name, separator, source_name = item.partition("=")
        if not separator or not child_name or not source_name:
            raise typer.BadParameter("--env-from entries must use CHILD=SOURCE")
        if child_name in references:
            raise typer.BadParameter(f"--env-from child name is repeated: {child_name}")
        references[child_name] = source_name
    return references


def _artifact_use_refs(items: list[str] | None) -> list[ArtifactUse]:
    """Parse legacy shorthand or canonical JSON artifact dependency bindings."""
    refs: list[ArtifactUse] = []
    for item in items or []:
        try:
            if item.lstrip().startswith("{"):
                refs.append(ArtifactUse.model_validate_json(item))
            else:
                artifact_id, separator, sha256 = item.partition("=")
                if not separator or not artifact_id or not sha256:
                    raise ValueError(
                        "dependency must use ARTIFACT_ID=SHA256 or a canonical JSON object"
                    )
                refs.append(ArtifactUse(artifact_id=artifact_id, sha256=sha256))
        except ValueError as exc:
            raise typer.BadParameter(
                str(exc),
                param_hint="--used-artifact",
            ) from exc
    artifact_ids = [ref.artifact_id for ref in refs]
    if len(artifact_ids) != len(set(artifact_ids)):
        raise typer.BadParameter(
            "--used-artifact values must have unique artifact IDs",
            param_hint="--used-artifact",
        )
    canonical = sorted(refs, key=lambda ref: ref.artifact_id)
    try:
        validate_artifact_use_collection(canonical)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--used-artifact") from exc
    return canonical


def _artifact_use_cli_value(ref: ArtifactUse) -> str:
    """Render legacy shorthand or canonical JSON for one CLI dependency."""
    if ref.provenance is None:
        return f"{ref.artifact_id}={ref.sha256}"
    return json.dumps(
        artifact_use_payload(ref),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _artifact_use_idempotency_suffix(refs: list[ArtifactUse]) -> str:
    """Return a stable suffix only when a submission has artifact dependencies."""
    if not refs:
        return ""
    payload = [artifact_use_payload(ref) for ref in refs]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f":uses-{hashlib.sha256(encoded).hexdigest()}"

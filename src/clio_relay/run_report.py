"""Write, load and render run reports.

A run report is the JSON evidence record the worker's session, transport,
deploy and relay-host commands leave behind (see
:mod:`clio_relay.run_report_models` for its shape and
:mod:`clio_relay.run_report_recorder` for how a run fills it in).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pydantic import ValidationError

from clio_relay.durable_validation_write import atomic_write_text
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.redaction import redact_sensitive_values
from clio_relay.run_report_models import LiveValidationReport


def _utc_now() -> datetime:
    return datetime.now(UTC)


def default_report_path(cluster: str, *, root: Path | None = None) -> Path:
    """Return a collision-resistant local path for a validation JSON report."""
    directory = root or Path(".clio-relay") / "validation-reports"
    timestamp = _utc_now().strftime("%Y%m%dT%H%M%S.%fZ")
    safe_cluster = "".join(char if char.isalnum() or char in "-_" else "-" for char in cluster)
    return directory / f"validation-{timestamp}-{safe_cluster}-{uuid4().hex[:8]}.json"


def write_validation_report(report: LiveValidationReport, path: Path) -> None:
    """Write a report atomically with deterministic JSON field ordering."""
    validated = LiveValidationReport.model_validate(report.model_dump(mode="python"))
    payload = redact_sensitive_values(validated.model_dump(mode="json"))
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def load_validation_report(path: Path) -> LiveValidationReport:
    """Load and strictly validate a report from disk."""
    logical_path = logical_filesystem_path(path)
    try:
        report = LiveValidationReport.model_validate_json(
            internal_filesystem_path(logical_path, force_extended=True).read_text(encoding="utf-8")
        )
    except (OSError, ValidationError) as exc:
        raise ConfigurationError(f"could not read validation report {logical_path}: {exc}") from exc
    report._source_path = logical_path  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    return report


def render_validation_markdown(report: LiveValidationReport) -> str:
    """Render a concise human-readable view of the canonical JSON report."""
    lines = [
        f"# live validation {report.report_id}",
        "",
        f"- status: `{report.status.value}`",
        f"- scenario: `{report.scenario}`",
        f"- cluster: `{report.cluster}`",
        f"- clio-relay: `{report.software.version}`",
        f"- commit: `{report.software.commit or 'unknown'}`",
        f"- install: `{report.install_source.kind.value}` via `{report.install_source.launcher}`",
        "",
        "## checks",
        "",
    ]
    lines.extend(
        f"- `{check.status.value}` `{check.check_id}`: {check.summary}" for check in report.checks
    )
    lines.extend(["", "## resources", ""])
    lines.extend(
        f"- `{resource.kind}` `{resource.resource_id}`"
        + (f" ({resource.state})" if resource.state is not None else "")
        for resource in report.resources
    )
    if report.error is not None:
        lines.extend(["", "## failure", "", f"`{report.error}`"])
    return "\n".join(lines) + "\n"

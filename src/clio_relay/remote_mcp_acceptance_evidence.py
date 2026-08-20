"""Bounded evidence-projection primitives shared by the acceptance validators.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns the small, dependency-free
helpers that read one ordinary
:class:`~clio_relay.remote_mcp_acceptance_models.RemoteMcpAcceptanceReport`
or raw structured-result value and project it into a bounded, evidence-safe
form -- retaining a durable-artifact-friendly excerpt rather than an
unbounded raw value. The Spack transition report/check family and the
structured-result validators (both later, separately extracted owner
modules) build on these.

None of these ten names have a caller outside ``remote_mcp.py`` (confirmed
by grep before the move; other split owner modules import them directly from
here, not from ``remote_mcp.py``), so ``remote_mcp.py`` imports them directly
rather than re-exporting them.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any, cast

from clio_relay.remote_mcp_acceptance_models import (
    RemoteMcpAcceptanceReport,
    _is_canonical_absolute_posix_path,
)
from clio_relay.remote_mcp_schema_validation import _bounded_diagnostic
from clio_relay.remote_mcp_stdio_evidence import _as_json

JSON = dict[str, Any]


def _transition_call_arguments(report: RemoteMcpAcceptanceReport) -> JSON:
    """Return the ordinary report's MCP call arguments when structurally present."""
    spec = _as_json(report.call_job.get("spec")) or {}
    return _as_json(spec.get("arguments")) or {}


def _bounded_transition_arguments(arguments: JSON, tool: str) -> JSON:
    """Retain only operation-defining scalar arguments in transition evidence."""
    keys = {"spack_find": ("query",), "spack_install": ("spec", "reuse")}.get(
        tool,
        ("spec",),
    )
    return {key: _bounded_evidence_scalar(arguments.get(key)) for key in keys}


def _bounded_spack_package_identity(package: JSON | None) -> JSON:
    """Return bounded Spack package identity fields for durable evidence."""
    if package is None:
        return {}
    return {
        key: _bounded_evidence_scalar(package.get(key))
        for key in ("name", "version", "dag_hash", "compiler", "architecture")
    }


def _bounded_evidence_scalar(value: object) -> object:
    """Bound strings retained in acceptance evidence while preserving scalar types."""
    if isinstance(value, str):
        return _bounded_diagnostic(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _bounded_diagnostic(type(value).__name__)


def _bounded_optional_string(value: object, maximum: int) -> str | None:
    """Return a string only when it is safe to retain in a bounded evidence model."""
    return value if isinstance(value, str) and len(value) <= maximum else None


def _acceptance_check_string(
    report: RemoteMcpAcceptanceReport,
    check_name: str,
    evidence_key: str,
) -> str | None:
    """Read one bounded string from a uniquely named passing acceptance check."""
    matches = [check for check in report.checks if check.name == check_name]
    if len(matches) != 1 or not matches[0].passed:
        return None
    return _bounded_optional_string(matches[0].evidence.get(evidence_key), 128)


def _acceptance_server_artifact(report: RemoteMcpAcceptanceReport) -> JSON | None:
    """Return the exact verified call artifact from one ordinary acceptance report."""
    matches = [check for check in report.checks if check.name == "remote-mcp.server-artifact"]
    if len(matches) != 1 or not matches[0].passed:
        return None
    artifact = matches[0].evidence.get("call_server_artifact")
    return cast(JSON, artifact) if isinstance(artifact, dict) else None


def _same_nonempty_strings(values: tuple[str | None, ...]) -> bool:
    """Return whether all values are the same non-empty string."""
    return all(isinstance(value, str) and bool(value) for value in values) and len(set(values)) == 1


def _common_string(values: tuple[str | None, ...]) -> str | None:
    """Return one common non-empty value, or ``None`` when identity is ambiguous."""
    return values[0] if _same_nonempty_strings(values) else None


def _is_strict_canonical_posix_descendant(path: object, root: object) -> bool:
    """Return whether ``path`` is canonical and strictly contained by ``root``."""
    if not _is_canonical_absolute_posix_path(path) or not _is_canonical_absolute_posix_path(root):
        return False
    typed_path = PurePosixPath(cast(str, path))
    typed_root = PurePosixPath(cast(str, root))
    return typed_path != typed_root and typed_root in typed_path.parents

"""Inspect a second Python distribution without importing its application code.

Extracted from ``installation.py`` (iowarp/clio-relay#231): small, focused
probes over an installed distribution's own metadata (PEP 610 direct-url,
package-progress entry points) plus the launcher/interpreter binding check
-- the counterpart to ``wheel_record_closure.py``'s much larger byte-level
RECORD verification, which this module does not perform.
"""

from __future__ import annotations

import json
import os
from importlib import metadata
from pathlib import Path
from typing import cast

from clio_relay.bounded_process import BoundedProcessError, run_bounded_process
from clio_relay.distribution_source_identity import verify_distribution_file_source
from clio_relay.errors import ConfigurationError


def _distribution_progress_entry_points(distribution: metadata.Distribution) -> list[str]:
    """Return stable package-progress entry-point identities for one distribution."""
    return sorted(
        f"{entry_point.group}:{entry_point.name}"
        for entry_point in distribution.entry_points
        if entry_point.group == "clio_relay.package_progress_adapters"
    )


def _distribution_direct_url(distribution: metadata.Distribution) -> dict[str, object]:
    """Return a normalized PEP 610 direct-url document when present."""
    direct_url_text = distribution.read_text("direct_url.json")
    if direct_url_text is None:
        return {}
    try:
        loaded = json.loads(direct_url_text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {str(key): value for key, value in cast(dict[object, object], loaded).items()}


def _direct_url_source_matches(value: object, *, expected_artifact: Path) -> bool:
    """Return whether direct-url evidence resolves to one exact local artifact."""
    document: object = {"url": value} if isinstance(value, str) else value
    try:
        direct_url_text = json.dumps(document, sort_keys=True)
        verify_distribution_file_source(
            direct_url_text=direct_url_text,
            expected_artifact=expected_artifact,
        )
    except (ConfigurationError, TypeError, ValueError):
        return False
    return True


def _probe_python_distribution(python: str | None, distribution_name: str) -> dict[str, object]:
    """Inspect a second interpreter without importing provider application code."""
    if python is None:
        return {"verified": False, "error": "execution interpreter is not configured"}
    script = """
import json
import sys
from importlib import metadata

distribution = metadata.distribution(sys.argv[1])
direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
entry_points = sorted(
    f"{entry_point.group}:{entry_point.name}"
    for entry_point in distribution.entry_points
    if entry_point.group == "clio_relay.package_progress_adapters"
)
print(json.dumps({
    "executable": sys.executable,
    "distribution": distribution.name,
    "distribution_version": distribution.version,
    "direct_url": direct_url.get("url"),
    "entry_points": entry_points,
}, sort_keys=True))
"""
    try:
        completed = run_bounded_process(
            [python, "-c", script, distribution_name],
            timeout_seconds=10,
            stdout_maximum_bytes=1024 * 1024,
            stderr_maximum_bytes=16 * 1024,
        )
    except (OSError, BoundedProcessError) as exc:
        return {"verified": False, "error": f"{type(exc).__name__}: {exc}"}
    if completed.returncode != 0:
        return {
            "verified": False,
            "error": completed.stderr.strip() or completed.stdout.strip(),
        }
    try:
        loaded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        return {"verified": False, "error": f"invalid interpreter probe JSON: {exc}"}
    if not isinstance(loaded, dict):
        return {"verified": False, "error": "interpreter probe was not an object"}
    return {str(key): value for key, value in cast(dict[object, object], loaded).items()}


def _jarvis_executable_matches_interpreter(
    executable: str | None,
    python: str | None,
    *,
    runtime_command: list[str],
) -> bool:
    """Require the configured JARVIS launcher to live in the execution environment."""
    if executable is None or python is None or not runtime_command:
        return False
    executable_path = Path(executable).expanduser()
    python_path = Path(python).expanduser()
    try:
        execution_bin_directory = python_path.parent.resolve(strict=True)
        return (
            executable_path.is_file()
            and os.access(executable_path, os.X_OK)
            and executable_path.resolve(strict=True).parent == execution_bin_directory
            and Path(runtime_command[0]).expanduser().resolve(strict=True)
            == executable_path.resolve(strict=True)
        )
    except (OSError, RuntimeError, ValueError):
        return False


def _normalized_distribution_name(value: str) -> str:
    return value.lower().replace("_", "-").replace(".", "-")

"""Verify a local artifact against a distribution's recorded source URL.

Extracted from ``installation.py`` (iowarp/clio-relay#231): this is the one
piece of local-file provenance logic that both the persistent-uv-tool probe
(``persistent_uv_tool_probe.py``) and the python-distribution probe
(``python_distribution_probe.py``) depend on. Keeping it in its own leaf
module -- rather than folding it into either probe or leaving it resident in
the installation.py facade -- avoids a module-load circular import: both
probes need it, and installation.py's facade needs to import from both
probes for re-export.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import cast
from urllib.parse import unquote, urlsplit

from clio_relay.errors import ConfigurationError


def verify_distribution_file_source(
    *,
    direct_url_text: str | None,
    expected_artifact: Path,
) -> Path:
    """Verify that installed distribution metadata names one exact local artifact.

    Package installers preserve the lexical path supplied by the operator in
    ``direct_url.json``. Cluster home directories can be mounted through a
    different canonical path, so provenance identity must compare resolved
    filesystem paths instead of raw file-URL strings.
    """
    try:
        loaded = cast(object, json.loads(direct_url_text or "{}"))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("distribution direct_url.json is not valid JSON") from exc
    if not isinstance(loaded, dict):
        raise ConfigurationError("distribution direct_url.json must contain an object")
    payload = cast(dict[str, object], loaded)
    if not isinstance(source_url := payload.get("url"), str) or not source_url:
        raise ConfigurationError("distribution direct_url.json does not name a source URL")
    try:
        parsed = urlsplit(source_url)
    except ValueError as exc:
        raise ConfigurationError("distribution source URL is not valid") from exc
    if parsed.scheme.lower() != "file":
        raise ConfigurationError("distribution source URL is not a local file URL")
    if parsed.netloc:
        raise ConfigurationError("distribution source file URL must not contain an authority")
    if parsed.query or parsed.fragment:
        raise ConfigurationError("distribution source file URL must not contain query or fragment")
    source_path_text = unquote(parsed.path)
    if os.name == "nt" and re.fullmatch(r"/[A-Za-z]:/.*", source_path_text):
        source_path_text = source_path_text[1:]
    if not source_path_text:
        raise ConfigurationError("distribution source file URL has no path")
    source_artifact = Path(source_path_text)
    if not source_artifact.is_absolute():
        raise ConfigurationError("distribution source file URL path must be absolute")
    try:
        expected = expected_artifact.resolve(strict=True)
        source = source_artifact.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("distribution source artifact cannot be resolved") from exc
    if not expected.is_file() or not source.is_file():
        raise ConfigurationError("distribution source artifact is not a regular file")
    if source != expected:
        raise ConfigurationError("distribution source artifact does not match the verified wheel")
    return source

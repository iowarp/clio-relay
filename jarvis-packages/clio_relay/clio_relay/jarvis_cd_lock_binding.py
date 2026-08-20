"""Verification of the relay's pinned jarvis-cd wheel inside an embedded uv.lock.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). Pure
leaf validation -- no facade reach-back needed. The JARVIS-CD release pin
(``JARVIS_CD_VERSION`` and friends) lives here rather than in ``constants.py``
since it is owned by this verification concern specifically; these three names
must also mirror ``clio_relay.bootstrap`` byte-for-byte (a focused release test
prevents either copy from moving independently -- see the original module
docstring history in ``runner.py``/git blame), and the JARVIS package also runs
as a standalone repository package where importing the installed relay
bootstrap module is not a valid dependency boundary.
"""

from __future__ import annotations

import re
import tomllib
from typing import Any, cast

_JARVIS_CD_LOCK_BINDING_SCHEMA = "clio-relay.jarvis-cd-lock-binding.v1"

JARVIS_CD_VERSION = "1.8.0"
JARVIS_CD_WHEEL_URL = (
    "https://github.com/grc-iit/jarvis-cd/releases/download/"
    f"v{JARVIS_CD_VERSION}/jarvis_cd-{JARVIS_CD_VERSION}-py3-none-any.whl"
)
JARVIS_CD_WHEEL_SHA256 = "2c2e2042d0256bd3d9c117d75aaf00d26d9e814fcbcca9a904abf06399fc1067"


def _jarvis_cd_lock_binding(lock_content: bytes) -> dict[str, Any]:
    """Verify the unique JARVIS-CD wheel selected by one embedded uv lock."""
    evidence: dict[str, Any] = {
        "schema_version": _JARVIS_CD_LOCK_BINDING_SCHEMA,
        "dependency": "jarvis-cd",
        "expected_version": JARVIS_CD_VERSION,
        "expected_url": JARVIS_CD_WHEEL_URL,
        "expected_sha256": JARVIS_CD_WHEEL_SHA256,
        "jarvis_mcp_package_entry_count": 0,
        "resolved_dependency_entry_count": 0,
        "observed_resolved_dependency_entries": [],
        "metadata_requirement_entry_count": 0,
        "observed_metadata_requirement_entries": [],
        "observed_metadata_requirement_urls": [],
        "package_entry_count": 0,
        "wheel_entry_count": 0,
        "observed_version": None,
        "observed_source_url": None,
        "observed_wheel_url": None,
        "observed_wheel_sha256": None,
        "verified": False,
        "error": None,
    }
    try:
        document = tomllib.loads(lock_content.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        evidence["error"] = f"clio-kit JARVIS uv.lock is invalid: {exc}"
        return evidence
    raw_packages = document.get("package")
    if not isinstance(raw_packages, list):
        evidence["error"] = "clio-kit JARVIS uv.lock omitted package records"
        return evidence
    package_records = [
        cast(dict[str, Any], value)
        for value in cast(list[object], raw_packages)
        if isinstance(value, dict)
    ]
    jarvis_mcp_packages = [
        value
        for value in package_records
        if _normalized_distribution_name(value.get("name")) == "jarvis-mcp"
    ]
    evidence["jarvis_mcp_package_entry_count"] = len(jarvis_mcp_packages)
    if len(jarvis_mcp_packages) != 1:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock must contain exactly one jarvis-mcp package record"
        )
        return evidence
    raw_dependencies = jarvis_mcp_packages[0].get("dependencies")
    dependencies = (
        cast(list[object], raw_dependencies) if isinstance(raw_dependencies, list) else []
    )
    jarvis_cd_dependencies = [
        cast(dict[str, Any], value)
        for value in dependencies
        if isinstance(value, dict)
        and _normalized_distribution_name(cast(dict[str, Any], value).get("name")) == "jarvis-cd"
    ]
    evidence["resolved_dependency_entry_count"] = len(jarvis_cd_dependencies)
    evidence["observed_resolved_dependency_entries"] = [
        _lock_entry_evidence(value, expected_fields=("name",)) for value in jarvis_cd_dependencies
    ]
    if len(jarvis_cd_dependencies) != 1:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp must resolve exactly one direct "
            "jarvis-cd dependency"
        )
        return evidence
    if jarvis_cd_dependencies[0] != {"name": "jarvis-cd"}:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp resolved jarvis-cd dependency must be unconditional"
        )
        return evidence
    raw_metadata = jarvis_mcp_packages[0].get("metadata")
    metadata_value = cast(dict[str, Any], raw_metadata) if isinstance(raw_metadata, dict) else None
    raw_requirements = metadata_value.get("requires-dist") if metadata_value is not None else None
    requirements = (
        cast(list[object], raw_requirements) if isinstance(raw_requirements, list) else []
    )
    jarvis_cd_requirements = [
        cast(dict[str, Any], value)
        for value in requirements
        if isinstance(value, dict)
        and _normalized_distribution_name(cast(dict[str, Any], value).get("name")) == "jarvis-cd"
    ]
    evidence["metadata_requirement_entry_count"] = len(jarvis_cd_requirements)
    evidence["observed_metadata_requirement_entries"] = [
        _lock_entry_evidence(value, expected_fields=("name", "url"))
        for value in jarvis_cd_requirements
    ]
    evidence["observed_metadata_requirement_urls"] = [
        _safe_observed_lock_text(value.get("url")) for value in jarvis_cd_requirements
    ]
    if len(jarvis_cd_requirements) != 1:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp metadata must contain exactly one "
            "jarvis-cd requirement"
        )
        return evidence
    if jarvis_cd_requirements[0].get("url") != JARVIS_CD_WHEEL_URL:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp metadata jarvis-cd URL does not match relay pin"
        )
        return evidence
    if jarvis_cd_requirements[0] != {
        "name": "jarvis-cd",
        "url": JARVIS_CD_WHEEL_URL,
    }:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp metadata jarvis-cd requirement "
            "must be an unconditional direct URL"
        )
        return evidence
    packages = [
        value
        for value in package_records
        if _normalized_distribution_name(value.get("name")) == "jarvis-cd"
    ]
    evidence["package_entry_count"] = len(packages)
    if len(packages) != 1:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock must contain exactly one jarvis-cd package record"
        )
        return evidence
    package = packages[0]
    version = package.get("version")
    evidence["observed_version"] = _safe_observed_lock_text(version)
    raw_source = package.get("source")
    source = cast(dict[str, Any], raw_source) if isinstance(raw_source, dict) else None
    source_url = source.get("url") if source is not None else None
    evidence["observed_source_url"] = _safe_observed_lock_text(source_url)
    raw_wheels = package.get("wheels")
    wheels = cast(list[object], raw_wheels) if isinstance(raw_wheels, list) else []
    evidence["wheel_entry_count"] = len(wheels)
    if len(wheels) == 1 and isinstance(wheels[0], dict):
        wheel = cast(dict[str, Any], wheels[0])
        wheel_url = wheel.get("url")
        wheel_hash = wheel.get("hash")
        evidence["observed_wheel_url"] = _safe_observed_lock_text(wheel_url)
        if isinstance(wheel_hash, str) and wheel_hash.startswith("sha256:"):
            evidence["observed_wheel_sha256"] = wheel_hash.removeprefix("sha256:")
    if version != JARVIS_CD_VERSION:
        evidence["error"] = "clio-kit JARVIS uv.lock jarvis-cd version does not match relay pin"
        return evidence
    if not isinstance(source_url, str) or source_url != JARVIS_CD_WHEEL_URL:
        evidence["error"] = "clio-kit JARVIS uv.lock jarvis-cd source URL does not match relay pin"
        return evidence
    if len(wheels) != 1 or not isinstance(wheels[0], dict):
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-cd must contain exactly one wheel record"
        )
        return evidence
    wheel_url = evidence["observed_wheel_url"]
    if wheel_url != source_url:
        evidence["error"] = "clio-kit JARVIS uv.lock jarvis-cd source and wheel URLs do not match"
        return evidence
    if wheel_url != JARVIS_CD_WHEEL_URL:
        evidence["error"] = "clio-kit JARVIS uv.lock jarvis-cd wheel URL does not match relay pin"
        return evidence
    wheel_sha256 = evidence["observed_wheel_sha256"]
    if wheel_sha256 != JARVIS_CD_WHEEL_SHA256:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-cd wheel SHA-256 does not match relay pin"
        )
        return evidence
    evidence["verified"] = True
    return evidence


def _lock_entry_evidence(
    value: dict[str, Any],
    *,
    expected_fields: tuple[str, ...],
) -> dict[str, Any]:
    """Project one TOML table into bounded, always-JSON-safe lock evidence."""
    evidence: dict[str, Any] = {}
    for field_name in expected_fields:
        if field_name in value:
            evidence[field_name] = _safe_observed_lock_text(value[field_name])
    unexpected_fields = sorted(set(value).difference(expected_fields))
    if unexpected_fields:
        evidence["unexpected_field_count"] = len(unexpected_fields)
        evidence["unexpected_fields"] = unexpected_fields[:32]
    return evidence


def _safe_observed_lock_text(value: object) -> str | None:
    """Keep expected lock text verbatim and safely identify every other TOML type."""
    if value is None or isinstance(value, str):
        return value
    return f"<invalid TOML {type(value).__name__}>"


def _normalized_distribution_name(value: object) -> str | None:
    """Return the normalized distribution name used by Python package metadata."""
    if not isinstance(value, str) or not value:
        return None
    return re.sub(r"[-_.]+", "-", value).lower()


def _require_locked_jarvis_cd_binding(
    server_artifact: dict[str, Any],
    *,
    expected: dict[str, str],
) -> None:
    """Refuse the built-in locked clio-kit JARVIS server when its JARVIS pin drifts."""
    raw_runtime = server_artifact.get("nested_runtime")
    if not isinstance(raw_runtime, dict):
        raise ValueError("built-in JARVIS MCP server omitted locked clio-kit runtime evidence")
    runtime = cast(dict[str, Any], raw_runtime)
    if runtime.get("server_name") != "jarvis":
        raise ValueError("built-in JARVIS MCP server did not select the locked jarvis runtime")
    raw_binding = runtime.get("jarvis_cd_lock_binding")
    binding = cast(dict[str, Any], raw_binding) if isinstance(raw_binding, dict) else None
    if (
        server_artifact.get("verified") is True
        and runtime.get("schema_version") == "clio-kit.locked-server.v4"
        and runtime.get("locked_runtime_verified") is True
        and binding is not None
        and binding.get("schema_version") == _JARVIS_CD_LOCK_BINDING_SCHEMA
        and binding.get("dependency") == "jarvis-cd"
        and binding.get("verified") is True
        and binding.get("error") is None
        and binding.get("expected_version") == expected["version"]
        and binding.get("expected_url") == expected["url"]
        and binding.get("expected_sha256") == expected["sha256"]
        and binding.get("observed_version") == expected["version"]
        and binding.get("observed_source_url") == expected["url"]
        and binding.get("observed_wheel_url") == expected["url"]
        and binding.get("observed_wheel_sha256") == expected["sha256"]
        and binding.get("resolved_dependency_entry_count") == 1
        and binding.get("observed_resolved_dependency_entries") == [{"name": "jarvis-cd"}]
        and binding.get("jarvis_mcp_package_entry_count") == 1
        and binding.get("metadata_requirement_entry_count") == 1
        and binding.get("observed_metadata_requirement_entries")
        == [{"name": "jarvis-cd", "url": expected["url"]}]
        and binding.get("observed_metadata_requirement_urls") == [expected["url"]]
        and binding.get("package_entry_count") == 1
        and binding.get("wheel_entry_count") == 1
    ):
        return
    if server_artifact.get("verified") is not True:
        reason = (
            server_artifact.get("identity_error")
            or server_artifact.get("error")
            or "outer MCP server artifact did not verify"
        )
    elif runtime.get("schema_version") != "clio-kit.locked-server.v4":
        reason = "locked clio-kit runtime schema did not verify"
    elif runtime.get("locked_runtime_verified") is not True:
        reason = runtime.get("error") or "locked clio-kit launcher/runtime did not verify"
    elif binding is None:
        reason = "JARVIS-CD lock binding evidence is missing"
    else:
        reason = binding.get("error") or "JARVIS-CD lock binding evidence did not match"
    raise ValueError(
        f"built-in locked clio-kit JARVIS MCP has an unverified jarvis-cd dependency: {reason}"
    )


def _jarvis_cd_lock_expectation(value: object) -> dict[str, str] | None:
    """Validate the explicit built-in JARVIS dependency expectation from the relay spec."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("expected_jarvis_cd_lock_binding must be an object")
    typed = cast(dict[object, object], value)
    expected = {
        "schema_version": "clio-relay.jarvis-cd-lock-expectation.v1",
        "version": JARVIS_CD_VERSION,
        "url": JARVIS_CD_WHEEL_URL,
        "sha256": JARVIS_CD_WHEEL_SHA256,
    }
    if typed != expected:
        raise ValueError("expected_jarvis_cd_lock_binding does not match the relay release pin")
    return expected

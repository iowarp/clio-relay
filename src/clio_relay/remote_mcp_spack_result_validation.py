"""Per-operation Spack structured-result validation (find/locate/install).

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns the exact semantic checks for
one Spack ``structuredContent`` payload against the configured
:class:`~clio_relay.remote_mcp_acceptance_models.RemoteMcpStructuredResultExpectation`
-- the three ``_validate_spack_*_result`` functions this file's
``_spack_structured_result_check`` caller (in
``remote_mcp_structured_result.py``) dispatches to per tool -- plus the
shared package-identity primitives (record narrowing, expected-identity
match, and the bounded identity projection) both they and the fresh-install
transition checks (``remote_mcp_spack_transition_checks.py``) build on.

None of these seven names have a caller outside ``remote_mcp.py`` (confirmed
by grep before the move; other split owner modules import them directly from
here, not from ``remote_mcp.py``), so ``remote_mcp.py`` imports them directly
rather than re-exporting them.
"""

from __future__ import annotations

import math
from typing import Any, cast

from clio_relay.remote_mcp_acceptance_models import (
    RemoteMcpStructuredResultExpectation,
    _is_canonical_absolute_posix_path,
)
from clio_relay.remote_mcp_stdio_evidence import _as_json

JSON = dict[str, Any]


def _validate_spack_find_result(
    structured: JSON,
    arguments: JSON,
    expectation: RemoteMcpStructuredResultExpectation,
    failures: list[str],
    observed: JSON,
) -> None:
    """Validate a Spack find result and record bounded evidence."""
    packages = _spack_package_records(structured.get("packages"))
    count = structured.get("count")
    expected_query = arguments.get("query")
    observed.update(
        {
            "query": structured.get("query"),
            "count": count,
        }
    )
    if structured.get("query") != expected_query:
        failures.append("find result query does not match the submitted query")
    if packages is None:
        failures.append("find result packages is not an array of objects")
        return
    if not isinstance(count, int) or isinstance(count, bool) or count != len(packages):
        failures.append("find result count does not match the package array")
    _record_expected_spack_package(packages, expectation, failures, observed)


def _validate_spack_locate_result(
    structured: JSON,
    arguments: JSON,
    expectation: RemoteMcpStructuredResultExpectation,
    failures: list[str],
    observed: JSON,
) -> None:
    """Validate one unique Spack package, prefix, and canonical load spec."""
    expected_spec = expectation.requested_spec
    package = _as_json(structured.get("package"))
    prefix = structured.get("prefix")
    load_spec = structured.get("load_spec")
    expected_load_spec = f"/{expectation.dag_hash}"
    canonical_prefix = _is_canonical_absolute_posix_path(prefix)
    prefix_matches_expected = prefix == expectation.prefix
    package_matches = package is not None and _spack_package_matches(package, expectation)
    observed.update(
        {
            "requested_spec": structured.get("requested_spec"),
            "load_spec": load_spec,
            "prefix": prefix,
            "prefix_is_canonical_absolute": canonical_prefix,
            "prefix_matches_expected": prefix_matches_expected,
            "package": _spack_package_identity(package),
            "expected_package_match_count": 1 if package_matches else 0,
        }
    )
    if arguments.get("spec") != expected_spec:
        failures.append("submitted locate spec does not match the configured expectation")
    if structured.get("requested_spec") != expected_spec:
        failures.append("locate result requested_spec does not match the expectation")
    if load_spec != expected_load_spec:
        failures.append("locate result load_spec is not the canonical /dag_hash")
    if not canonical_prefix:
        failures.append("locate result prefix is not a canonical absolute POSIX path")
    if not prefix_matches_expected:
        failures.append("locate result prefix does not match the configured exact prefix")
    if not package_matches:
        failures.append("locate result package does not match the expected name and DAG hash")


def _validate_spack_install_result(
    structured: JSON,
    arguments: JSON,
    expectation: RemoteMcpStructuredResultExpectation,
    failures: list[str],
    observed: JSON,
) -> None:
    """Validate installed/reused status and the observed exact Spack identity."""
    expected_spec = expectation.requested_spec
    packages = _spack_package_records(structured.get("packages"))
    duration = structured.get("duration_seconds")
    observed.update(
        {
            "requested_spec": structured.get("requested_spec"),
            "reuse": structured.get("reuse"),
            "status": structured.get("status"),
            "duration_seconds": duration,
        }
    )
    if arguments.get("spec") != expected_spec or arguments.get("reuse") is not expectation.reuse:
        failures.append("submitted install arguments do not match the configured expectation")
    if structured.get("requested_spec") != expected_spec:
        failures.append("install result requested_spec does not match the expectation")
    if structured.get("reuse") is not expectation.reuse:
        failures.append("install result reuse does not match the expectation")
    if structured.get("status") != "installed":
        failures.append("install result does not report installed status")
    if (
        not isinstance(duration, (int, float))
        or isinstance(duration, bool)
        or not math.isfinite(duration)
        or duration < 0
    ):
        failures.append("install result duration_seconds is not a finite non-negative number")
    if packages is None:
        failures.append("install result packages is not an array of objects")
        return
    _record_expected_spack_package(packages, expectation, failures, observed)


def _record_expected_spack_package(
    packages: list[JSON],
    expectation: RemoteMcpStructuredResultExpectation,
    failures: list[str],
    observed: JSON,
) -> None:
    """Record and require one exact package identity without retaining an unbounded list."""
    matches = [package for package in packages if _spack_package_matches(package, expectation)]
    named_packages = [
        package for package in packages if package.get("name") == expectation.package_name
    ]
    named_hashes = sorted(
        {
            str(package["dag_hash"])
            for package in packages
            if package.get("name") == expectation.package_name
            and isinstance(package.get("dag_hash"), str)
        }
    )
    observed["package_count"] = len(packages)
    observed["expected_package_match_count"] = len(matches)
    observed["expected_package_name_count"] = len(named_packages)
    observed["package_hashes_for_expected_name"] = named_hashes[:20]
    if len(matches) != 1:
        failures.append("result does not contain exactly one expected package name and DAG hash")
    if named_hashes != [expectation.dag_hash]:
        failures.append("result contains an unexpected or ambiguous hash for the package name")
    if len(named_packages) != 1:
        failures.append("result does not contain one unique package record for the package name")
    if len(packages) != 1:
        failures.append("result does not contain exactly one matching root package")


def _spack_package_records(value: object) -> list[JSON] | None:
    """Return typed Spack package records only when every array item is an object."""
    if not isinstance(value, list):
        return None
    records: list[JSON] = []
    for item in cast(list[object], value):
        record = _as_json(item)
        if record is None:
            return None
        records.append(record)
    return records


def _spack_package_matches(
    package: JSON,
    expectation: RemoteMcpStructuredResultExpectation,
) -> bool:
    """Return whether one package has the exact configured stable identity."""
    return (
        package.get("name") == expectation.package_name
        and package.get("dag_hash") == expectation.dag_hash
    )


def _spack_package_identity(package: JSON | None) -> JSON:
    """Return the bounded identity fields needed in release evidence."""
    if package is None:
        return {}
    return {
        key: package.get(key) for key in ("name", "version", "dag_hash", "compiler", "architecture")
    }

"""Bind reports and policy pins to one physical cluster target identity (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). A
:class:`~clio_relay.validation_schema.ReleaseGatePolicy` with
``require_target_identity`` names, for each cluster, an independently pinned
physical machine (hostnames, SSH host-key fingerprints, scheduler provider,
a site marker) as a :class:`~clio_relay.validation_schema.
ReleaseTargetIdentity`. This module is the one owner for turning that pin
and a report's observed ``cluster_target`` resource into the same canonical,
hashed identity so a release gate can prove every report ran against the
declared machine rather than trusting the cluster label alone.

:func:`policy_target_identity_digests` and :func:`report_set_target_identities`
are the two entry points gate evaluation calls; :func:`report_target_identity`
validates and hashes one report's observed target and compares it against
its policy pin; :func:`validated_policy_target` independently validates and
hashes the pin itself (a pin containing a ``PENDING`` sentinel is accepted as
declared-but-not-yet-verifiable, never as a match).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from typing import Any, cast

from clio_relay.validation_schema import (
    LiveValidationReport,
    ReleaseGatePolicy,
    ReleaseTargetIdentity,
    ValidationStatus,
)


def policy_target_identity_digests(policy: ReleaseGatePolicy) -> dict[str, str]:
    """Return only policy target digests proven to match their canonical fields."""
    digests: dict[str, str] = {}
    for label, target in sorted(policy.targets.items()):
        _, digest, failures = validated_policy_target(target)
        if digest is not None and not failures:
            digests[label] = digest
    return digests


def report_set_target_identities(
    policy: ReleaseGatePolicy,
    reports: Iterable[LiveValidationReport],
) -> tuple[dict[str, str], list[str]]:
    """Bind exact policy target coverage to policy-pinned physical identities."""
    digests_by_cluster: dict[str, set[str]] = {}
    failures: list[str] = []
    report_list = list(reports)
    observed_clusters = {report.cluster for report in report_list if report.cluster != "local"}
    policy_clusters = set(policy.targets)
    missing_clusters = sorted(policy_clusters - observed_clusters)
    extra_clusters = sorted(observed_clusters - policy_clusters)
    if missing_clusters:
        failures.append(f"policy targets lack report coverage: {missing_clusters}")
    if extra_clusters:
        failures.append(f"reports reference targets absent from policy: {extra_clusters}")
    for report in report_list:
        if report.cluster == "local":
            continue
        digest, report_failures = report_target_identity(
            report,
            policy.targets.get(report.cluster),
        )
        failures.extend(
            f"report {report.report_id} for cluster {report.cluster}: {failure}"
            for failure in report_failures
        )
        if digest is not None:
            digests_by_cluster.setdefault(report.cluster, set()).add(digest)
    stable: dict[str, str] = {}
    for cluster, digests in sorted(digests_by_cluster.items()):
        if len(digests) == 1:
            stable[cluster] = next(iter(digests))
            continue
        failures.append(
            f"cluster {cluster} reports identify different physical target identities: "
            f"{sorted(digests)}"
        )
    return stable, sorted(set(failures))


def report_target_identity(
    report: LiveValidationReport,
    policy_target: ReleaseTargetIdentity | None,
) -> tuple[str | None, list[str]]:
    """Validate an observed target and compare it with the independent policy pin."""
    failures: list[str] = []
    passed_checks = {
        check.check_id
        for check in report.checks
        if check.status is ValidationStatus.PASSED and check.evidence
    }
    if "worker.target-identity" not in passed_checks:
        failures.append("missing evidenced worker.target-identity check")
    targets = [resource for resource in report.resources if resource.kind == "cluster_target"]
    if len(targets) != 1:
        failures.append(f"must identify exactly one cluster_target resource; found {len(targets)}")
        return None, failures
    target = targets[0]
    if target.cluster != report.cluster:
        failures.append("cluster_target resource does not match the report cluster")
    if target.role != "physical_cluster_target":
        failures.append("cluster_target resource is not a physical_cluster_target")
    if target.state != "verified":
        failures.append("cluster_target resource state is not verified")
    metadata = target.metadata
    if metadata.get("verified") is not True:
        failures.append("cluster_target metadata is not verified")
    if metadata.get("schema_version") != "clio-relay.cluster-target-info.v1":
        failures.append("cluster_target schema version does not match")

    observed_hostnames = {
        normalized
        for key in ("hostname", "fqdn")
        if isinstance((value := metadata.get(key)), str)
        and (normalized := normalized_hostname(value))
    }
    observed_fingerprints = target_identity_string_set(
        metadata.get("ssh_host_key_sha256"),
        field="ssh_host_key_sha256",
        failures=failures,
    )
    if not observed_hostnames:
        failures.append("cluster_target must identify an observed hostname or FQDN")

    provider = target.provider
    observed_provider = metadata.get("scheduler_provider")
    if not isinstance(provider, str) or not provider.strip():
        failures.append("cluster_target resource omits its scheduler provider")
    elif observed_provider != provider:
        failures.append("cluster_target scheduler provider does not match its metadata")

    observed_scheduler = metadata.get("scheduler_cluster_name")
    if observed_scheduler is not None and (
        not isinstance(observed_scheduler, str) or not observed_scheduler.strip()
    ):
        failures.append("scheduler_cluster_name must be a non-empty string or null")

    observed_site_marker = metadata.get("site_marker_sha256")
    if (
        not isinstance(observed_site_marker, str)
        or re.fullmatch(r"[0-9a-fA-F]{64}", observed_site_marker) is None
    ):
        failures.append("site_marker_sha256 must identify the observed physical target")

    if (
        failures
        or not observed_hostnames
        or not observed_fingerprints
        or not isinstance(provider, str)
        or not isinstance(observed_site_marker, str)
    ):
        return None, failures
    canonical = {
        "schema_version": "clio-relay.cluster-target-identity.v1",
        "observed_hostnames": sorted(observed_hostnames),
        "observed_ssh_host_key_sha256": sorted(observed_fingerprints),
        "scheduler_cluster_name": (
            observed_scheduler.strip() if isinstance(observed_scheduler, str) else None
        ),
        "site_marker_sha256": observed_site_marker.lower(),
        "scheduler_provider": provider.strip().lower(),
    }
    digest = canonical_target_identity_sha256(canonical)
    if policy_target is None:
        failures.append("cluster label has no independently pinned policy target")
        return digest, failures
    pinned_canonical, pinned_digest, pin_failures = validated_policy_target(policy_target)
    failures.extend(pin_failures)
    if pinned_canonical is not None and canonical != pinned_canonical:
        differing_fields = sorted(
            key for key in canonical if canonical.get(key) != pinned_canonical.get(key)
        )
        failures.append(
            f"observed physical target does not match policy-pinned fields: {differing_fields}"
        )
    if pinned_digest is not None and digest != pinned_digest:
        failures.append("observed physical target digest does not match the policy pin")
    return digest, failures


def validated_policy_target(
    target: ReleaseTargetIdentity,
) -> tuple[dict[str, Any] | None, str | None, list[str]]:
    """Validate a target pin and bind its declared digest to its canonical fields."""
    values = [
        *target.hostnames,
        *target.ssh_host_key_sha256,
        target.scheduler_provider,
        target.site_marker_sha256,
        target.identity_sha256,
    ]
    if target.scheduler_cluster_name is not None:
        values.append(target.scheduler_cluster_name)
    if any(value.strip().upper().startswith("PENDING") for value in values):
        return None, None, ["policy target identity contains a PENDING pin"]
    failures: list[str] = []
    if re.fullmatch(r"[0-9a-fA-F]{64}", target.site_marker_sha256) is None:
        failures.append("policy target site_marker_sha256 is not a SHA-256 digest")
    if re.fullmatch(r"[0-9a-fA-F]{64}", target.identity_sha256) is None:
        failures.append("policy target identity_sha256 is not a SHA-256 digest")
    if failures:
        return None, None, failures
    canonical: dict[str, Any] = {
        "schema_version": "clio-relay.cluster-target-identity.v1",
        "observed_hostnames": sorted(normalized_hostname(item) for item in target.hostnames),
        "observed_ssh_host_key_sha256": sorted(item.strip() for item in target.ssh_host_key_sha256),
        "scheduler_cluster_name": (
            target.scheduler_cluster_name.strip()
            if target.scheduler_cluster_name is not None
            else None
        ),
        "site_marker_sha256": target.site_marker_sha256.lower(),
        "scheduler_provider": target.scheduler_provider.strip().lower(),
    }
    computed_digest = canonical_target_identity_sha256(canonical)
    declared_digest = target.identity_sha256.lower()
    if computed_digest != declared_digest:
        failures.append("policy target identity_sha256 does not match its pinned fields")
    return canonical, declared_digest, failures


def canonical_target_identity_sha256(canonical: dict[str, Any]) -> str:
    """Hash one normalized physical target identity deterministically."""
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def target_identity_string_set(
    value: object,
    *,
    field: str,
    failures: list[str],
    normalize_hostname: bool = False,
) -> set[str]:
    """Validate a non-empty unique string list used in a target identity."""
    if not isinstance(value, list) or not value:
        failures.append(f"cluster_target {field} must be a non-empty list")
        return set()
    raw_items = cast(list[object], value)
    if any(not isinstance(item, str) or not item.strip() for item in raw_items):
        failures.append(f"cluster_target {field} contains a blank or non-string value")
        return set()
    normalized = {
        normalized_hostname(item) if normalize_hostname else item.strip()
        for item in cast(list[str], raw_items)
    }
    if "" in normalized or len(normalized) != len(raw_items):
        failures.append(f"cluster_target {field} contains duplicate or invalid values")
        return set()
    return normalized


def normalized_hostname(value: str) -> str:
    """Normalize hostnames for case-insensitive physical identity comparison."""
    return value.strip().rstrip(".").lower()

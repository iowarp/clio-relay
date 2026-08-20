"""Match a release requirement's stateful resources and JARVIS execution (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). Beyond the plain
required-resource-*kind* check gate evaluation does inline,
:class:`~clio_relay.validation_schema.ReleaseGateRequirement` can name
stateful ``required_resources`` predicates (role/state/provider/metadata
constraints with a minimum count) and require every one of them stay scoped
to the requirement's own cluster. :func:`required_resource_failures` and
:func:`requirement_resource_scope_failures` are the two entry points gate
evaluation calls for that. :func:`jarvis_execution_identity_failures` is a
narrower, JARVIS-specific binding: when a requirement asks for
``jarvis_execution_progress`` evidence, every execution-scoped resource and
structured JARVIS check must agree on exactly one ``execution_id``, so
evidence from two unrelated JARVIS runs can never be combined into one
apparent pass.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import cast

from clio_relay.validation_schema import (
    LiveValidationReport,
    ReleaseGateRequirement,
    ReleaseResourceRequirement,
    ValidationResource,
    ValidationStatus,
)

_JARVIS_EXECUTION_CHECK_IDS = frozenset(
    {
        "jarvis.structured-runtime-metadata",
        "remote-mcp.jarvis-execution-query",
        "remote-mcp.jarvis-live-progress",
    }
)
_JARVIS_EXECUTION_RELAY_JOB_ROLES = frozenset(
    {"jarvis_mcp_execution_query", "virtual_jarvis_mcp_call"}
)


def required_resource_failures(
    requirement: ReleaseGateRequirement,
    resources_to_check: Iterable[ValidationResource],
    *,
    expected_cluster: str,
) -> list[str]:
    """Return failures for stateful resource predicates in a release policy."""
    resources = list(resources_to_check)
    failures: list[str] = []
    for required in requirement.required_resources:
        matching = matching_required_resources(
            required,
            resources,
            expected_cluster=expected_cluster,
        )
        if len(matching) >= required.minimum_count:
            continue
        constraints: list[str] = []
        if required.roles is not None:
            constraints.append(f"roles={required.roles}")
        if required.states is not None:
            constraints.append(f"states={required.states}")
        if required.providers is not None:
            constraints.append(f"providers={required.providers}")
        if required.metadata_equals:
            constraints.append(f"metadata_equals={required.metadata_equals}")
        suffix = f" ({', '.join(constraints)})" if constraints else ""
        failures.append(
            f"requires {required.minimum_count} matching {required.kind} resource(s){suffix}; "
            f"found {len(matching)}"
        )
    return failures


def matching_required_resources(
    required: ReleaseResourceRequirement,
    resources: Iterable[ValidationResource],
    *,
    expected_cluster: str,
) -> list[ValidationResource]:
    """Return resources matching one predicate on the exact policy target."""
    return [
        resource
        for resource in resources
        if resource.cluster == expected_cluster
        and resource.kind == required.kind
        and (required.roles is None or resource.role in required.roles)
        and (required.states is None or resource.state in required.states)
        and (required.providers is None or resource.provider in required.providers)
        and all(
            _metadata_value_matches(resource.metadata.get(key), expected)
            for key, expected in required.metadata_equals.items()
        )
    ]


def requirement_resource_scope_failures(
    requirement: ReleaseGateRequirement,
    resources_to_check: Iterable[ValidationResource],
) -> list[str]:
    """Reject required evidence kinds attributed to any other target or no target."""
    target_scoped_kinds = {
        *requirement.required_resource_kinds,
        *(required.kind for required in requirement.required_resources),
    }
    mismatched = sorted(
        {
            f"{resource.kind}:{resource.resource_id}:{resource.cluster or '<unscoped>'}"
            for resource in resources_to_check
            if resource.kind in target_scoped_kinds and resource.cluster != requirement.cluster
        }
    )
    if not mismatched:
        return []
    return [
        f"required evidence resources must belong to cluster {requirement.cluster}: {mismatched}"
    ]


def jarvis_execution_identity_failures(
    requirement: ReleaseGateRequirement,
    report: LiveValidationReport,
) -> list[str]:
    """Bind JARVIS checks and semantic resources to one durable execution."""
    if "jarvis_execution_progress" not in {
        *requirement.required_resource_kinds,
        *(required.kind for required in requirement.required_resources),
    }:
        return []

    failures: list[str] = []
    execution_ids: set[str] = set()
    identity_requirements = [
        required
        for required in requirement.required_resources
        if required.kind in {"jarvis_execution_progress", "jarvis_generated_artifact"}
        or (
            required.kind == "relay_job"
            and required.roles is not None
            and bool(_JARVIS_EXECUTION_RELAY_JOB_ROLES.intersection(required.roles))
        )
    ]
    for required in identity_requirements:
        for resource in matching_required_resources(
            required,
            report.resources,
            expected_cluster=requirement.cluster,
        ):
            execution_id = resource.metadata.get("execution_id")
            if not isinstance(execution_id, str) or not execution_id:
                failures.append(
                    "JARVIS execution-scoped resource omits execution_id: "
                    f"{resource.kind}:{resource.resource_id}"
                )
                continue
            execution_ids.add(execution_id)

    if len(execution_ids) != 1:
        failures.append(
            "JARVIS execution-scoped resources do not identify exactly one execution: "
            f"{sorted(execution_ids)}"
        )
        return failures
    expected_execution_id = next(iter(execution_ids))

    for check_id in sorted(_JARVIS_EXECUTION_CHECK_IDS.intersection(requirement.required_checks)):
        checks = [
            check
            for check in report.checks
            if check.check_id == check_id
            and check.status is ValidationStatus.PASSED
            and check.evidence
        ]
        if len(checks) != 1:
            failures.append(
                f"JARVIS execution check {check_id} must appear exactly once in the report"
            )
            continue
        evidence_ids = [evidence.metadata.get("execution_id") for evidence in checks[0].evidence]
        if (
            not evidence_ids
            or any(not isinstance(value, str) or not value for value in evidence_ids)
            or set(cast(list[str], evidence_ids)) != {expected_execution_id}
        ):
            failures.append(
                f"JARVIS execution check {check_id} is not bound to "
                f"execution {expected_execution_id}"
            )
    return failures


def _metadata_value_matches(observed: object, expected: object) -> bool:
    """Match nested metadata dictionaries as required subsets and other values exactly."""
    if isinstance(expected, dict):
        if not isinstance(observed, dict):
            return False
        typed_expected = cast(dict[object, object], expected)
        typed_observed = cast(dict[object, object], observed)
        return all(
            key in typed_observed and _metadata_value_matches(typed_observed[key], expected_value)
            for key, expected_value in typed_expected.items()
        )
    return observed == expected

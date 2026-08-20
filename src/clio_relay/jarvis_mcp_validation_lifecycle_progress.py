"""Post-run JARVIS execution-query lifecycle and package-progress evidence.

Owner module for the ``jarvis_mcp_validation.py`` split (clio-relay split/
jarvis-mcp-validation): validates in-flight and terminal package progress
sampled through repeated ``jarvis_get_execution`` observations -- query
identity coherence, scheduler identity stability, the durable-state lifecycle
prefix, and monotonic package-progress-event advancement across the observed
span, including the relay's own bounded fail-closed integrity/verified-gap
markers. ``build_jarvis_mcp_validation_report`` in
``jarvis_mcp_validation_report.py`` calls
``_jarvis_query_lifecycle_progress_evidence`` for the resumable
``jarvis_get_execution`` lifecycle path; the facade re-exports it under this
exact private name because ``tests/test_jarvis_mcp_validation.py`` calls it
directly via ``jarvis_validation._jarvis_query_lifecycle_progress_evidence``.
"""

from __future__ import annotations

from typing import cast

from clio_relay.jarvis_mcp_validation_core import (
    _UNBOUND_JARVIS_IDENTITY,
    JSON,
    _is_sha256,
    _mapping,
    _nonnegative_int,
    _positive_int,
)
from clio_relay.jarvis_mcp_validation_progress_semantics import (
    _compact_package_progress,
    _jarvis_progress_semantic_signature,
    _jarvis_progress_transition_nonregressing,
    _valid_jarvis_progress_semantics,
)


def _valid_query_integrity_marker(value: object) -> bool:
    """Recognize only the relay's bounded fail-closed integrity accumulator."""
    marker = _mapping(value)
    if marker is None:
        return False
    return bool(
        set(marker) == {"schema_version", "valid", "reason", "previous_state", "current_state"}
        and marker.get("schema_version") == "clio-relay.jarvis-query-integrity.v1"
        and marker.get("valid") is False
        and isinstance(marker.get("reason"), str)
        and marker.get("reason")
        and (marker.get("previous_state") is None or isinstance(marker.get("previous_state"), str))
        and (marker.get("current_state") is None or isinstance(marker.get("current_state"), str))
    )


def _valid_query_verified_gap(
    value: object,
    *,
    previous: JSON,
    current: JSON,
) -> bool:
    """Validate one relay-trusted local summary for a sampled observation span."""
    marker = _mapping(value)
    if marker is None:
        return False
    return bool(
        set(marker)
        == {
            "schema_version",
            "verified",
            "discarded_observation_count",
            "discarded_observations_sha256",
            "previous_query_job_id",
            "current_query_job_id",
        }
        and marker.get("schema_version") == "clio-relay.jarvis-query-verified-gap.v1"
        and marker.get("verified") is True
        and _positive_int(marker.get("discarded_observation_count"))
        and _is_sha256(marker.get("discarded_observations_sha256"))
        and marker.get("previous_query_job_id") == previous.get("query_job_id")
        and marker.get("current_query_job_id") == current.get("query_job_id")
    )


def _jarvis_query_lifecycle_progress_evidence(
    *,
    observations: list[JSON],
    pipeline_id: object,
    execution_id: object,
    scheduler_cluster: object = _UNBOUND_JARVIS_IDENTITY,
    scheduler_provider: object = _UNBOUND_JARVIS_IDENTITY,
) -> tuple[JSON, bool, JSON | None]:
    """Validate in-flight and terminal progress obtained through execution queries."""
    state_rank = {
        "preparing": 0,
        "scripted": 1,
        "submitting": 2,
        "submitted": 3,
        "running": 4,
        "completed": 5,
        "failed": 5,
        "canceled": 5,
    }
    normalized: list[tuple[JSON, JSON, JSON, JSON]] = []
    identities_valid = isinstance(pipeline_id, str) and bool(pipeline_id)
    identities_valid = identities_valid and isinstance(execution_id, str) and bool(execution_id)
    expected_mode = "scheduler" if scheduler_provider is not None else "direct"
    bounded = 0 < len(observations) <= 512
    for observation in observations:
        handle = _mapping(observation.get("execution_handle"))
        record = _mapping(observation.get("execution_record"))
        progress = _mapping(observation.get("progress"))
        if handle is None or record is None or progress is None:
            identities_valid = False
            continue
        if not (
            observation.get("pipeline_id") == pipeline_id
            and observation.get("execution_id") == execution_id
            and record.get("pipeline_id") == pipeline_id
            and record.get("execution_id") == execution_id
            and handle.get("pipeline_id") == pipeline_id
            and handle.get("execution_id") == execution_id
            and progress.get("pipeline_id") == pipeline_id
            and progress.get("execution_id") == execution_id
            and (
                scheduler_provider is _UNBOUND_JARVIS_IDENTITY
                or (
                    handle.get("mode") == expected_mode
                    and record.get("mode") == expected_mode
                    and handle.get("scheduler_provider") == scheduler_provider
                    and record.get("scheduler_provider") == scheduler_provider
                )
            )
            and handle.get("schema_version") == "jarvis.execution.handle.v1"
            and record.get("state") == observation.get("state")
            and record.get("terminal") is observation.get("terminal")
            and progress.get("execution_state") == observation.get("state")
            and progress.get("terminal") is observation.get("terminal")
            and record.get("schema_version") == "jarvis.execution.record.v1"
            and progress.get("schema_version") == "jarvis.execution.progress.v1"
            and isinstance(observation.get("query_job_id"), str)
            and bool(observation.get("query_job_id"))
        ):
            identities_valid = False
        normalized.append((observation, handle, record, progress))

    base_identity_fields = (
        "execution_id",
        "pipeline_id",
        "mode",
        "scheduler_provider",
    )
    snapshot_identity_fields = (*base_identity_fields, "scheduler_native_id", "cluster")
    stable_identity: tuple[object, ...] | None = None
    assigned_scheduler_native_id: str | None = None
    assigned_scheduler_cluster: str | None = None
    scheduler_identity_valid = bool(
        scheduler_cluster is _UNBOUND_JARVIS_IDENTITY
        or scheduler_cluster is None
        or (isinstance(scheduler_cluster, str) and bool(scheduler_cluster))
    )
    for observation, handle, record, _progress in normalized:
        identity = tuple(handle.get(field) for field in base_identity_fields)
        if stable_identity is None:
            stable_identity = identity
        elif identity != stable_identity:
            scheduler_identity_valid = False
        if any(handle.get(field) != record.get(field) for field in snapshot_identity_fields):
            scheduler_identity_valid = False
        mode = handle.get("mode")
        provider = handle.get("scheduler_provider")
        native_id = handle.get("scheduler_native_id")
        native_cluster = handle.get("cluster")
        if mode == "direct":
            if provider is not None or native_id is not None or native_cluster is not None:
                scheduler_identity_valid = False
            continue
        if mode != "scheduler" or not isinstance(provider, str) or not provider:
            scheduler_identity_valid = False
            continue
        if native_id is None:
            if assigned_scheduler_native_id is not None or observation.get("terminal") is True:
                scheduler_identity_valid = False
        elif not isinstance(native_id, str) or not native_id:
            scheduler_identity_valid = False
        elif assigned_scheduler_native_id is None:
            assigned_scheduler_native_id = native_id
        elif native_id != assigned_scheduler_native_id:
            scheduler_identity_valid = False
        if native_cluster is None:
            if assigned_scheduler_cluster is not None:
                scheduler_identity_valid = False
        elif not isinstance(native_cluster, str) or not native_cluster:
            scheduler_identity_valid = False
        elif assigned_scheduler_cluster is None:
            assigned_scheduler_cluster = native_cluster
        elif native_cluster != assigned_scheduler_cluster:
            scheduler_identity_valid = False
    if scheduler_cluster is not _UNBOUND_JARVIS_IDENTITY:
        if scheduler_cluster is None:
            if assigned_scheduler_cluster is not None:
                scheduler_identity_valid = False
        elif assigned_scheduler_cluster != scheduler_cluster:
            scheduler_identity_valid = False
    integrity_violations: list[JSON] = []
    for observation, _handle, _record, _progress in normalized:
        marker = observation.get("relay_query_integrity")
        if marker is None:
            continue
        if _valid_query_integrity_marker(marker):
            integrity_violations.append(cast(JSON, marker))
        else:
            integrity_violations.append(
                {
                    "schema_version": "clio-relay.jarvis-query-integrity.v1",
                    "valid": False,
                    "reason": "integrity_marker_invalid",
                    "previous_state": None,
                    "current_state": observation.get("state"),
                }
            )
    verified_gap_counts: list[int] = []
    invalid_verified_gaps: list[JSON] = []
    verified_gap_count = 0
    for index, (observation, _handle, _record, _progress) in enumerate(normalized):
        marker = observation.get("relay_query_verified_gap")
        if marker is not None:
            if index == 0 or not _valid_query_verified_gap(
                marker,
                previous=normalized[index - 1][0],
                current=observation,
            ):
                invalid_verified_gaps.append(
                    {
                        "index": index,
                        "query_job_id": observation.get("query_job_id"),
                    }
                )
            else:
                verified_gap_count += 1
        verified_gap_counts.append(verified_gap_count)
    lifecycle_prefix_valid = bool(
        normalized
        and identities_valid
        and scheduler_identity_valid
        and not integrity_violations
        and not invalid_verified_gaps
    )
    last_known_rank = -1
    terminal_snapshot: tuple[object, object, object] | None = None
    terminal_seen = False
    for index, (observation, _handle, record, _progress) in enumerate(normalized):
        state = observation.get("state")
        if state == "unknown":
            if (
                observation.get("terminal") is not False
                or index == len(normalized) - 1
                or terminal_seen
            ):
                lifecycle_prefix_valid = False
            continue
        rank = state_rank.get(state) if isinstance(state, str) else None
        if rank is None:
            lifecycle_prefix_valid = False
            continue
        if rank < last_known_rank:
            lifecycle_prefix_valid = False
        last_known_rank = max(last_known_rank, rank)
        terminal = observation.get("terminal")
        if terminal is True:
            if state not in {"completed", "failed", "canceled"}:
                lifecycle_prefix_valid = False
            snapshot = (state, record.get("return_code"), record.get("error"))
            if terminal_snapshot is None:
                terminal_snapshot = snapshot
            elif snapshot != terminal_snapshot:
                lifecycle_prefix_valid = False
            terminal_seen = True
        elif terminal is False:
            if (
                terminal_seen
                or state in {"completed", "failed", "canceled"}
                or record.get("return_code") is not None
                or record.get("error") is not None
            ):
                lifecycle_prefix_valid = False
        else:
            lifecycle_prefix_valid = False
    terminal_success_valid = bool(
        lifecycle_prefix_valid
        and normalized[-1][0].get("state") == "completed"
        and normalized[-1][0].get("terminal") is True
        and normalized[-1][2].get("return_code") == 0
        and normalized[-1][2].get("error") is None
    )

    live_package: tuple[int, JSON, JSON, JSON] | None = None
    progress_monotonic = True
    package_counters: dict[tuple[object, object], tuple[int, int, JSON | None, int]] = {}
    for observation_index, (
        observation,
        _handle,
        _record,
        execution_progress,
    ) in enumerate(normalized):
        packages = execution_progress.get("packages")
        if not isinstance(packages, list):
            progress_monotonic = False
            continue
        for raw_package in cast(list[object], packages):
            package = _mapping(raw_package)
            if package is None:
                progress_monotonic = False
                continue
            key = (package.get("package_id"), package.get("package_name"))
            event_count = package.get("event_count")
            latest = _mapping(package.get("latest"))
            base_valid = bool(
                isinstance(key[0], str)
                and bool(key[0])
                and isinstance(key[1], str)
                and bool(key[1])
                and _nonnegative_int(event_count)
            )
            if not base_valid:
                progress_monotonic = False
                continue
            if event_count == 0 and latest is None:
                counters = (0, -1)
            elif latest is not None and bool(
                _nonnegative_int(latest.get("sequence"))
                and latest.get("schema_version") == "jarvis.progress.v1"
                and latest.get("execution_id") == execution_id
                and latest.get("package_id") == key[0]
                and latest.get("package_name") == key[1]
                and _valid_jarvis_progress_semantics(latest)
            ):
                counters = (cast(int, event_count), cast(int, latest["sequence"]))
            else:
                progress_monotonic = False
                continue
            prior = package_counters.get(key)
            if prior is not None:
                if counters[0] < prior[0] or counters[1] < prior[1]:
                    progress_monotonic = False
                prior_latest = prior[2]
                crossed_verified_gap = verified_gap_counts[observation_index] > prior[3]
                if prior_latest is not None and latest is not None:
                    if counters[1] == prior[1]:
                        if counters[0] != prior[0] or _jarvis_progress_semantic_signature(
                            latest
                        ) != _jarvis_progress_semantic_signature(prior_latest):
                            progress_monotonic = False
                    elif counters[1] > prior[1] and (
                        counters[0] <= prior[0]
                        or (
                            not crossed_verified_gap
                            and not _jarvis_progress_transition_nonregressing(
                                prior_latest,
                                latest,
                            )
                        )
                    ):
                        progress_monotonic = False
            package_counters[key] = (
                counters[0],
                counters[1],
                latest,
                verified_gap_counts[observation_index],
            )
        if observation.get("state") != "running" or observation.get("terminal") is not False:
            continue
        for raw_package in cast(list[object], packages):
            package = _mapping(raw_package)
            latest = _mapping(package.get("latest")) if package else None
            if (
                package is not None
                and latest is not None
                and _positive_int(package.get("event_count"))
                and latest.get("schema_version") == "jarvis.progress.v1"
                and latest.get("execution_id") == execution_id
                and latest.get("package_id") == package.get("package_id")
                and latest.get("package_name") == package.get("package_name")
                and _valid_jarvis_progress_semantics(latest)
                and _nonnegative_int(latest.get("sequence"))
            ):
                live_package = (observation_index, observation, package, latest)

    terminal_package: tuple[JSON, JSON] | None = None
    if live_package is not None and normalized:
        live_summary = live_package[2]
        terminal_packages = normalized[-1][3].get("packages")
        if isinstance(terminal_packages, list):
            for raw_package in cast(list[object], terminal_packages):
                package = _mapping(raw_package)
                latest = _mapping(package.get("latest")) if package else None
                if (
                    package is not None
                    and latest is not None
                    and package.get("package_id") == live_summary.get("package_id")
                    and package.get("package_name") == live_summary.get("package_name")
                ):
                    terminal_package = (package, latest)
                    break

    progress_valid = False
    if live_package is not None and terminal_package is not None:
        live_index, _live_observation, live_summary, live_latest = live_package
        terminal_summary, terminal_latest = terminal_package
        progress_valid = bool(
            terminal_latest.get("schema_version") == "jarvis.progress.v1"
            and terminal_latest.get("execution_id") == execution_id
            and terminal_latest.get("package_id") == live_latest.get("package_id")
            and terminal_latest.get("package_name") == live_latest.get("package_name")
            and _nonnegative_int(terminal_summary.get("event_count"))
            and cast(int, terminal_summary["event_count"]) >= cast(int, live_summary["event_count"])
            and _nonnegative_int(terminal_latest.get("sequence"))
            and cast(int, terminal_latest["sequence"]) >= cast(int, live_latest["sequence"])
            and _valid_jarvis_progress_semantics(terminal_latest)
            and (
                verified_gap_counts[-1] > verified_gap_counts[live_index]
                or _jarvis_progress_transition_nonregressing(live_latest, terminal_latest)
            )
        )

    compact_observations = [
        {
            "query_job_id": observation.get("query_job_id"),
            "state": observation.get("state"),
            "terminal": observation.get("terminal"),
            "query_integrity": observation.get("relay_query_integrity"),
            "verified_gap": observation.get("relay_query_verified_gap"),
            "package_count": (
                len(cast(list[object], progress.get("packages")))
                if isinstance(progress.get("packages"), list)
                else None
            ),
        }
        for observation, _handle, _record, progress in normalized
    ]
    observations_truncated = len(compact_observations) > 32
    if observations_truncated:
        compact_observations = [*compact_observations[:31], compact_observations[-1]]
    assertions = {
        "observation_count_bounded": bounded,
        "query_identities_coherent": identities_valid,
        "scheduler_identity_optional_coherent_and_stable": scheduler_identity_valid,
        "lifecycle_prefix_coherent": lifecycle_prefix_valid,
        "terminal_success_verified": terminal_success_valid,
        "in_flight_package_progress_observed": live_package is not None,
        "package_progress_nonregressing": progress_monotonic,
        "terminal_package_progress_bound": progress_valid,
    }
    evidence: JSON = {
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "observation_count": len(observations),
        "observations": compact_observations,
        "observations_truncated": observations_truncated,
        "query_integrity_violations": integrity_violations,
        "verified_gap_count": verified_gap_count,
        "invalid_verified_gaps": invalid_verified_gaps,
        "live_progress": (
            _compact_package_progress(live_package[3]) if live_package is not None else {}
        ),
        "terminal_progress": (
            _compact_package_progress(terminal_package[1]) if terminal_package is not None else {}
        ),
        "assertions": assertions,
    }
    passed = all(assertions.values())
    if live_package is None or terminal_package is None:
        return evidence, passed, None
    _live_index, live_observation, _live_summary, live_latest = live_package
    terminal_summary, terminal_latest = terminal_package
    resource: JSON = {
        "resource_id": f"{execution_id}:{live_latest.get('package_id', 'package')}",
        "provider": "jarvis-cd",
        "metadata": {
            "source": "jarvis_get_execution",
            "pipeline_id": pipeline_id,
            "execution_id": execution_id,
            "package_id": live_latest.get("package_id"),
            "package_name": live_latest.get("package_name"),
            "progress_schema_version": live_latest.get("schema_version"),
            "progress_determinate": live_latest.get("determinate"),
            "progress_event_count": terminal_summary.get("event_count"),
            "progress_sequence": terminal_latest.get("sequence"),
            "provider_source_authority": "jarvis_get_execution",
            "native_documents_validated": identities_valid,
            "query_identity_validated": identities_valid,
            "live_observed_while_running": True,
            "lifecycle_prefix_validated": lifecycle_prefix_valid,
            "terminal_success_validated": terminal_success_valid,
            "terminal_query_bound": progress_valid,
            "live_query_job_id": live_observation.get("query_job_id"),
            "terminal_query_job_id": normalized[-1][0].get("query_job_id"),
        },
    }
    return evidence, passed, resource

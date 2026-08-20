"""JARVIS execution-query integrity and progress-observation tracking
(iowarp/clio-relay#231 continuation): the bounded observation ledger
and the package-progress/gap-marker integrity checks that guard it
against a regressing or corrupted remote report."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, cast


def _merge_jarvis_execution_query_observations(
    prior: list[dict[str, Any]],
    current: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge bounded query snapshots while preserving lifecycle order across retries."""
    merged: list[dict[str, Any]] = []
    for observation in [*prior, *current]:
        _append_bounded_jarvis_execution_query_observation(merged, observation)
    return merged


def _append_bounded_jarvis_execution_query_observation(
    observations: list[dict[str, Any]],
    observation: dict[str, Any],
) -> None:
    """Retain ordered lifecycle evidence without failing a healthy long run."""
    import clio_relay.cli as cli

    prior_violation = any(
        _valid_jarvis_query_integrity_marker(item.get(cli._JARVIS_QUERY_INTEGRITY_KEY))
        for item in observations
    )
    incoming_marker = observation.get(cli._JARVIS_QUERY_INTEGRITY_KEY)
    incoming_gap = observation.get(cli._JARVIS_VERIFIED_GAP_KEY)
    gap_invalid = incoming_gap is not None and (
        not observations
        or not _valid_jarvis_verified_gap_marker(
            incoming_gap,
            previous=observations[-1],
            current=observation,
        )
    )
    if gap_invalid:
        observation = {
            **observation,
            cli._JARVIS_QUERY_INTEGRITY_KEY: _jarvis_query_integrity_summary(
                "verified_gap_invalid",
                observations[-1].get("state") if observations else None,
                observation.get("state"),
            ),
        }
    elif incoming_marker is not None and not _valid_jarvis_query_integrity_marker(incoming_marker):
        observation = {
            **observation,
            cli._JARVIS_QUERY_INTEGRITY_KEY: _jarvis_query_integrity_summary(
                "integrity_marker_invalid",
                observations[-1].get("state") if observations else None,
                observation.get("state"),
            ),
        }
    elif not prior_violation and incoming_marker is None:
        violation = _jarvis_query_integrity_violation(
            observations,
            observation,
            crossed_verified_gap=incoming_gap is not None,
        )
        if violation is not None:
            observation = {**observation, cli._JARVIS_QUERY_INTEGRITY_KEY: violation}
    observations.append(observation)
    if len(observations) <= cli._MAX_JARVIS_EXECUTION_QUERY_OBSERVATIONS:
        return

    protected_indexes = {0, len(observations) - 1}
    first_state_indexes: dict[tuple[object, object], int] = {}
    first_live_progress_index: int | None = None
    for index, item in enumerate(observations):
        raw_state = item.get("state")
        state = (
            raw_state
            if raw_state in {"unknown", "submitted", "running", "completed", "failed", "canceled"}
            else "invalid"
        )
        state_key = (state, item.get("terminal"))
        first_state_indexes.setdefault(state_key, index)
        if _valid_jarvis_query_integrity_marker(item.get(cli._JARVIS_QUERY_INTEGRITY_KEY)):
            protected_indexes.add(index)
        if first_live_progress_index is None and _has_live_jarvis_package_progress(item):
            first_live_progress_index = index
    protected_indexes.update(first_state_indexes.values())
    if first_live_progress_index is not None:
        protected_indexes.add(first_live_progress_index)

    target_size = cli._MAX_JARVIS_EXECUTION_QUERY_OBSERVATIONS // 2
    available_slots = max(0, target_size - len(protected_indexes))
    candidates = [index for index in range(len(observations)) if index not in protected_indexes]
    if available_slots and candidates:
        selected = {
            candidates[index * len(candidates) // available_slots]
            for index in range(available_slots)
        }
        protected_indexes.update(selected)
    selected_indexes = sorted(protected_indexes)
    compacted: list[dict[str, Any]] = []
    prior_selected_index: int | None = None
    for index in selected_indexes:
        item = observations[index]
        if prior_selected_index is not None and index > prior_selected_index + 1:
            discarded = observations[prior_selected_index + 1 : index]
            item = {
                **item,
                cli._JARVIS_VERIFIED_GAP_KEY: _jarvis_verified_gap_marker(
                    previous=observations[prior_selected_index],
                    current=item,
                    discarded=discarded,
                ),
            }
        compacted.append(item)
        prior_selected_index = index
    observations[:] = compacted


def _jarvis_verified_gap_marker(
    *,
    previous: dict[str, Any],
    current: dict[str, Any],
    discarded: list[dict[str, Any]],
) -> dict[str, object]:
    """Record a relay-trusted summary after checking every sampled transition."""
    import clio_relay.cli as cli

    nested_gap = current.get(cli._JARVIS_VERIFIED_GAP_KEY)
    nested_discarded_count = (
        cast(dict[str, Any], nested_gap).get("discarded_observation_count")
        if isinstance(nested_gap, dict)
        else 0
    )
    if (
        not isinstance(nested_discarded_count, int)
        or isinstance(nested_discarded_count, bool)
        or nested_discarded_count < 0
    ):
        nested_discarded_count = 0
    canonical = json.dumps(
        {"discarded": discarded, "nested_current_gap": nested_gap},
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "schema_version": cli._JARVIS_VERIFIED_GAP_SCHEMA,
        "verified": True,
        "discarded_observation_count": len(discarded) + nested_discarded_count,
        "discarded_observations_sha256": hashlib.sha256(canonical).hexdigest(),
        "previous_query_job_id": previous.get("query_job_id"),
        "current_query_job_id": current.get("query_job_id"),
    }


def _valid_jarvis_verified_gap_marker(
    value: object,
    *,
    previous: dict[str, Any],
    current: dict[str, Any],
) -> bool:
    """Accept one exact relay-trusted local summary bound to adjacent retained snapshots."""
    import clio_relay.cli as cli

    if not isinstance(value, dict):
        return False
    marker = cast(dict[str, object], value)
    digest = marker.get("discarded_observations_sha256")
    count = marker.get("discarded_observation_count")
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
        and marker.get("schema_version") == cli._JARVIS_VERIFIED_GAP_SCHEMA
        and marker.get("verified") is True
        and isinstance(count, int)
        and not isinstance(count, bool)
        and count > 0
        and isinstance(digest, str)
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
        and marker.get("previous_query_job_id") == previous.get("query_job_id")
        and marker.get("current_query_job_id") == current.get("query_job_id")
    )


def _jarvis_query_integrity_violation(
    observations: list[dict[str, Any]],
    current: dict[str, Any],
    *,
    crossed_verified_gap: bool = False,
) -> dict[str, object] | None:
    """Return a sticky summary when compaction must not erase an integrity failure."""
    import clio_relay.cli as cli

    handle = current.get("execution_handle")
    record = current.get("execution_record")
    progress = current.get("progress")
    if (
        not isinstance(handle, dict)
        or not isinstance(record, dict)
        or not isinstance(progress, dict)
    ):
        return _jarvis_query_integrity_summary("native_document_missing", None, None)
    handle = cast(dict[str, Any], handle)
    record = cast(dict[str, Any], record)
    progress = cast(dict[str, Any], progress)
    expected_pipeline_id = current.get("pipeline_id")
    expected_execution_id = current.get("execution_id")
    if observations:
        expected_pipeline_id = observations[0].get("pipeline_id")
        expected_execution_id = observations[0].get("execution_id")
    if not (
        isinstance(current.get("query_job_id"), str)
        and bool(current.get("query_job_id"))
        and current.get("pipeline_id") == expected_pipeline_id
        and current.get("execution_id") == expected_execution_id
        and handle.get("pipeline_id") == expected_pipeline_id
        and handle.get("execution_id") == expected_execution_id
        and record.get("pipeline_id") == expected_pipeline_id
        and record.get("execution_id") == expected_execution_id
        and progress.get("pipeline_id") == expected_pipeline_id
        and progress.get("execution_id") == expected_execution_id
        and handle.get("schema_version") == "jarvis.execution.handle.v1"
        and record.get("schema_version") == "jarvis.execution.record.v1"
        and progress.get("schema_version") == "jarvis.execution.progress.v1"
        and record.get("state") == current.get("state")
        and record.get("terminal") is current.get("terminal")
        and progress.get("execution_state") == current.get("state")
        and progress.get("terminal") is current.get("terminal")
    ):
        return _jarvis_query_integrity_summary(
            "query_identity_changed",
            observations[-1].get("state") if observations else None,
            current.get("state"),
        )

    stable_fields = ("execution_id", "pipeline_id", "mode", "scheduler_provider")
    snapshot_fields = (*stable_fields, "scheduler_native_id", "cluster")
    if any(handle.get(field) != record.get(field) for field in snapshot_fields):
        return _jarvis_query_integrity_summary(
            "handle_record_identity_changed",
            observations[-1].get("state") if observations else None,
            current.get("state"),
        )
    if observations:
        first_handle = observations[0].get("execution_handle")
        if not isinstance(first_handle, dict):
            return _jarvis_query_integrity_summary(
                "durable_identity_changed",
                observations[-1].get("state"),
                current.get("state"),
            )
        typed_first_handle = cast(dict[str, Any], first_handle)
        if any(handle.get(field) != typed_first_handle.get(field) for field in stable_fields):
            return _jarvis_query_integrity_summary(
                "durable_identity_changed",
                observations[-1].get("state"),
                current.get("state"),
            )
    mode = handle.get("mode")
    provider = handle.get("scheduler_provider")
    native_id = handle.get("scheduler_native_id")
    scheduler_cluster = handle.get("cluster")
    if mode == "direct":
        if provider is not None or native_id is not None or scheduler_cluster is not None:
            return _jarvis_query_integrity_summary(
                "direct_scheduler_identity_present",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
    elif mode == "scheduler":
        if not isinstance(provider, str) or not provider:
            return _jarvis_query_integrity_summary(
                "scheduler_provider_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        assigned_native_id: object = None
        assigned_scheduler_cluster: object = None
        for item in observations:
            prior_handle = item.get("execution_handle")
            if not isinstance(prior_handle, dict):
                continue
            typed_prior_handle = cast(dict[str, Any], prior_handle)
            candidate_native_id = typed_prior_handle.get("scheduler_native_id")
            if candidate_native_id is not None:
                assigned_native_id = candidate_native_id
            candidate_scheduler_cluster = typed_prior_handle.get("cluster")
            if candidate_scheduler_cluster is not None:
                assigned_scheduler_cluster = candidate_scheduler_cluster
        if native_id is not None and (not isinstance(native_id, str) or not native_id):
            return _jarvis_query_integrity_summary(
                "scheduler_native_id_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        if assigned_native_id is not None and native_id != assigned_native_id:
            return _jarvis_query_integrity_summary(
                "scheduler_native_id_changed",
                observations[-1].get("state"),
                current.get("state"),
            )
        if scheduler_cluster is not None and (
            not isinstance(scheduler_cluster, str) or not scheduler_cluster
        ):
            return _jarvis_query_integrity_summary(
                "scheduler_cluster_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        if (
            assigned_scheduler_cluster is not None
            and scheduler_cluster != assigned_scheduler_cluster
        ):
            return _jarvis_query_integrity_summary(
                "scheduler_cluster_changed",
                observations[-1].get("state"),
                current.get("state"),
            )
    else:
        return _jarvis_query_integrity_summary(
            "execution_mode_invalid",
            observations[-1].get("state") if observations else None,
            current.get("state"),
        )

    current_state = current.get("state")
    current_terminal = current.get("terminal")
    current_rank = (
        cli._JARVIS_EXECUTION_STATE_RANK.get(current_state)
        if isinstance(current_state, str)
        else None
    )
    if current_state != "unknown" and current_rank is None:
        return _jarvis_query_integrity_summary("invalid_state", None, current_state)
    if current_terminal is True:
        if current_state not in {"completed", "failed", "canceled"}:
            return _jarvis_query_integrity_summary(
                "terminal_state_invalid",
                observations[-1].get("state") if observations else None,
                current_state,
            )
    elif current_terminal is False:
        if (
            current_state in {"completed", "failed", "canceled"}
            or record.get("return_code") is not None
            or record.get("error") is not None
        ):
            return _jarvis_query_integrity_summary(
                "nonterminal_result_present",
                observations[-1].get("state") if observations else None,
                current_state,
            )
    else:
        return _jarvis_query_integrity_summary(
            "terminal_flag_invalid",
            observations[-1].get("state") if observations else None,
            current_state,
        )

    prior_known = next(
        (
            item
            for item in reversed(observations)
            if item.get("state") in cli._JARVIS_EXECUTION_STATE_RANK
        ),
        None,
    )
    if prior_known is not None and current_rank is not None:
        prior_state = cast(str, prior_known["state"])
        if current_rank < cli._JARVIS_EXECUTION_STATE_RANK[prior_state]:
            return _jarvis_query_integrity_summary(
                "state_regression",
                prior_state,
                current_state,
            )

    prior_terminal = next(
        (item for item in observations if item.get("terminal") is True),
        None,
    )
    if prior_terminal is None:
        return _jarvis_package_progress_integrity_violation(
            observations,
            current,
            crossed_verified_gap=crossed_verified_gap,
        )
    if current_terminal is not True:
        return _jarvis_query_integrity_summary(
            "terminal_regression",
            prior_terminal.get("state"),
            current_state,
        )
    prior_record = prior_terminal.get("execution_record")
    current_record = current.get("execution_record")
    if not isinstance(prior_record, dict) or not isinstance(current_record, dict):
        return _jarvis_query_integrity_summary(
            "terminal_record_missing",
            prior_terminal.get("state"),
            current_state,
        )
    typed_prior_record = cast(dict[str, Any], prior_record)
    typed_current_record = cast(dict[str, Any], current_record)
    prior_result = (
        prior_terminal.get("state"),
        typed_prior_record.get("return_code"),
        typed_prior_record.get("error"),
    )
    current_result = (
        current_state,
        typed_current_record.get("return_code"),
        typed_current_record.get("error"),
    )
    if current_result != prior_result:
        return _jarvis_query_integrity_summary(
            "terminal_snapshot_changed",
            prior_terminal.get("state"),
            current_state,
        )
    return _jarvis_package_progress_integrity_violation(
        observations,
        current,
        crossed_verified_gap=crossed_verified_gap,
    )


def _jarvis_package_progress_integrity_violation(
    observations: list[dict[str, Any]],
    current: dict[str, Any],
    *,
    crossed_verified_gap: bool,
) -> dict[str, object] | None:
    """Reject package-progress corruption before bounded observation sampling."""
    progress = cast(dict[str, Any], current["progress"])
    packages = progress.get("packages")
    if not isinstance(packages, list):
        return _jarvis_query_integrity_summary(
            "package_progress_invalid",
            observations[-1].get("state") if observations else None,
            current.get("state"),
        )
    prior_packages: dict[tuple[object, object], tuple[int, int, dict[str, Any] | None]] = {}
    for observation in observations:
        prior_progress = observation.get("progress")
        if not isinstance(prior_progress, dict):
            continue
        typed_prior_progress = cast(dict[str, Any], prior_progress)
        if not isinstance(typed_prior_progress.get("packages"), list):
            continue
        for raw_package in cast(list[object], typed_prior_progress["packages"]):
            if not isinstance(raw_package, dict):
                continue
            package = cast(dict[str, Any], raw_package)
            summary = _jarvis_package_progress_summary(
                package,
                expected_execution_id=observation.get("execution_id"),
            )
            if summary is not None:
                prior_packages[(package.get("package_id"), package.get("package_name"))] = summary
    for raw_package in cast(list[object], packages):
        if not isinstance(raw_package, dict):
            return _jarvis_query_integrity_summary(
                "package_progress_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        package = cast(dict[str, Any], raw_package)
        summary = _jarvis_package_progress_summary(
            package,
            expected_execution_id=current.get("execution_id"),
        )
        if summary is None:
            return _jarvis_query_integrity_summary(
                "package_progress_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        key = (package.get("package_id"), package.get("package_name"))
        prior = prior_packages.get(key)
        if prior is None:
            continue
        event_count, sequence, latest = summary
        prior_event_count, prior_sequence, prior_latest = prior
        if event_count < prior_event_count or sequence < prior_sequence:
            return _jarvis_query_integrity_summary(
                "package_progress_regressed",
                observations[-1].get("state"),
                current.get("state"),
            )
        if latest is not None and prior_latest is not None:
            current_signature = _jarvis_package_progress_signature(latest)
            prior_signature = _jarvis_package_progress_signature(prior_latest)
            if sequence == prior_sequence and (
                event_count != prior_event_count or current_signature != prior_signature
            ):
                return _jarvis_query_integrity_summary(
                    "package_progress_changed_without_sequence",
                    observations[-1].get("state"),
                    current.get("state"),
                )
            if sequence > prior_sequence and (
                event_count <= prior_event_count
                or (
                    not crossed_verified_gap
                    and not _jarvis_package_progress_transition_nonregressing(
                        prior_latest,
                        latest,
                    )
                )
            ):
                return _jarvis_query_integrity_summary(
                    "package_progress_regressed",
                    observations[-1].get("state"),
                    current.get("state"),
                )
    return None


def _jarvis_package_progress_summary(
    package: dict[str, Any],
    *,
    expected_execution_id: object,
) -> tuple[int, int, dict[str, Any] | None] | None:
    """Return validated counters used by the query-integrity accumulator."""
    package_id = package.get("package_id")
    package_name = package.get("package_name")
    event_count = package.get("event_count")
    latest = package.get("latest")
    if (
        not isinstance(package_id, str)
        or not package_id
        or not isinstance(package_name, str)
        or not package_name
        or not isinstance(event_count, int)
        or isinstance(event_count, bool)
        or event_count < 0
    ):
        return None
    if event_count == 0 and latest is None:
        return 0, -1, None
    if not isinstance(latest, dict):
        return None
    typed_latest = cast(dict[str, Any], latest)
    sequence = typed_latest.get("sequence")
    if (
        typed_latest.get("schema_version") != "jarvis.progress.v1"
        or typed_latest.get("execution_id") != expected_execution_id
        or typed_latest.get("package_id") != package_id
        or typed_latest.get("package_name") != package_name
        or not isinstance(sequence, int)
        or isinstance(sequence, bool)
        or sequence < 0
        or not _jarvis_package_progress_semantics_valid(typed_latest)
    ):
        return None
    return event_count, sequence, typed_latest


def _jarvis_package_progress_semantics_valid(progress: dict[str, Any]) -> bool:
    """Validate package progress fields that could otherwise disappear during sampling."""
    import clio_relay.cli as cli

    state = progress.get("state")
    label = progress.get("label")
    if (
        state not in cli._JARVIS_PACKAGE_PROGRESS_STATES
        or not isinstance(label, str)
        or not label.strip()
        or len(label) > 256
    ):
        return False
    current = progress.get("current")
    total = progress.get("total")
    if current is not None and (
        isinstance(current, bool)
        or not isinstance(current, (int, float))
        or not math.isfinite(current)
        or current < 0
    ):
        return False
    if total is not None and (
        isinstance(total, bool)
        or not isinstance(total, (int, float))
        or not math.isfinite(total)
        or total <= 0
        or current is None
        or current > total
    ):
        return False
    unit = progress.get("unit")
    if unit is not None and (not isinstance(unit, str) or not unit.strip() or len(unit) > 256):
        return False
    determinate = progress.get("determinate")
    return isinstance(determinate, bool) and determinate is (
        current is not None and total is not None
    )


def _jarvis_package_progress_signature(progress: dict[str, Any]) -> tuple[object, ...]:
    """Return fields that cannot change without a new native progress sequence."""
    return tuple(
        progress.get(field)
        for field in ("state", "label", "determinate", "current", "total", "unit")
    )


def _jarvis_package_progress_transition_nonregressing(
    previous: dict[str, Any],
    current: dict[str, Any],
) -> bool:
    """Allow quantitative reset only after an explicit package phase change."""
    if (previous.get("state"), previous.get("label")) != (
        current.get("state"),
        current.get("label"),
    ):
        return True
    if previous.get("unit") is not None and current.get("unit") != previous.get("unit"):
        return False
    if previous.get("total") is not None and current.get("total") != previous.get("total"):
        return False
    previous_value = previous.get("current")
    current_value = current.get("current")
    return previous_value is None or bool(
        current_value is not None and cast(float, current_value) >= cast(float, previous_value)
    )


def _jarvis_query_integrity_summary(
    reason: str,
    previous_state: object,
    current_state: object,
) -> dict[str, object]:
    """Create one bounded machine-readable integrity accumulator entry."""
    import clio_relay.cli as cli

    return {
        "schema_version": cli._JARVIS_QUERY_INTEGRITY_SCHEMA,
        "valid": False,
        "reason": reason,
        "previous_state": previous_state,
        "current_state": current_state,
    }


def _valid_jarvis_query_integrity_marker(value: object) -> bool:
    """Accept only relay-generated, fail-closed query-integrity summaries."""
    import clio_relay.cli as cli

    if not isinstance(value, dict):
        return False
    marker = cast(dict[str, object], value)
    return bool(
        set(marker) == {"schema_version", "valid", "reason", "previous_state", "current_state"}
        and marker.get("schema_version") == cli._JARVIS_QUERY_INTEGRITY_SCHEMA
        and marker.get("valid") is False
        and isinstance(marker.get("reason"), str)
        and marker.get("reason")
        and (marker.get("previous_state") is None or isinstance(marker.get("previous_state"), str))
        and (marker.get("current_state") is None or isinstance(marker.get("current_state"), str))
    )


def _has_live_jarvis_package_progress(observation: dict[str, Any]) -> bool:
    """Return whether an in-flight observation contains native package progress."""
    if observation.get("state") != "running" or observation.get("terminal") is not False:
        return False
    progress = observation.get("progress")
    if not isinstance(progress, dict):
        return False
    packages = cast(dict[str, object], progress).get("packages")
    if not isinstance(packages, list):
        return False
    for raw_package in cast(list[object], packages):
        if not isinstance(raw_package, dict):
            continue
        package = cast(dict[str, object], raw_package)
        event_count = package.get("event_count")
        if (
            isinstance(event_count, int)
            and not isinstance(event_count, bool)
            and event_count > 0
            and isinstance(package.get("latest"), dict)
        ):
            return True
    return False

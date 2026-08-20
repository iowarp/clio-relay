"""Bounded remote/local collection pagination shared by owned-runtime
cleanup and the JARVIS execution-query engine (iowarp/clio-relay#231
continuation): the page-validated artifact/progress record readers and
terminal-job wait helpers both call."""

from __future__ import annotations

import json
import time
from json import JSONDecodeError
from typing import Any, cast

import clio_relay.cli_remote_worker_probe as cli_remote_worker_probe
import clio_relay.core_queue as core_queue
import clio_relay.relay_ops as relay_ops
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.errors import ConfigurationError, ObservationTimeoutError, RelayError
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS

MAX_INTERNAL_COLLECTION_RECORDS = 10_000


def _wait_for_remote_job_terminal(
    definition: ClusterDefinition,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
    deadline: float | None = None,
) -> dict[str, object]:
    """Wait for one remote relay job without requiring progress observations."""
    _validate_progress_wait(timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)
    timeout_deadline = time.monotonic() + timeout_seconds
    effective_deadline = timeout_deadline if deadline is None else min(timeout_deadline, deadline)
    while True:
        status = _json_output(
            cli_remote_worker_probe._run_remote_clio_before_deadline(
                definition,
                ["job", "status", job_id],
                deadline=effective_deadline,
            ),
            "JARVIS MCP execution-query job status",
        )
        if status.get("terminal") is True:
            return status
        remaining = effective_deadline - time.monotonic()
        if remaining <= 0:
            raise ObservationTimeoutError(
                f"job did not reach terminal state before timeout: {job_id}"
            )
        time.sleep(min(poll_seconds, remaining))


def _wait_for_local_job_terminal(
    queue: core_queue.ClioCoreQueue,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, object]:
    """Wait for one local relay job without requiring progress observations."""
    _validate_progress_wait(timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)
    deadline = time.monotonic() + timeout_seconds
    while True:
        status = relay_ops.job_status(queue, job_id)
        if status.get("terminal") is True:
            return status
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ObservationTimeoutError(
                f"job did not reach terminal state before timeout: {job_id}"
            )
        time.sleep(min(poll_seconds, remaining))


def _validate_progress_wait(*, timeout_seconds: float, poll_seconds: float) -> None:
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    if poll_seconds <= 0:
        raise ConfigurationError("poll_seconds must be positive")


def _json_value(value: str, label: str) -> object:
    try:
        return cast(object, json.loads(value))
    except JSONDecodeError as exc:
        raise RelayError(f"{label} did not return valid JSON: {exc.msg}") from exc


def _json_output(value: str, label: str) -> dict[str, object]:
    decoded = _json_value(value, label)
    if not isinstance(decoded, dict):
        raise RelayError(f"{label} did not return a JSON object")
    return {str(key): item for key, item in cast(dict[object, object], decoded).items()}


def _complete_local_artifact_records(
    queue: core_queue.ClioCoreQueue,
    job_id: str,
    *,
    max_records: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Read a complete bounded artifact snapshot or fail before using partial evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        page, next_cursor, total = queue.list_artifacts_page(
            job_id,
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
        )
        expected_total = _validate_complete_page(
            label=f"artifacts for {job_id}",
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
            max_records=max_records,
        )
        records.extend(item.model_dump(mode="json") for item in page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"artifacts for {job_id} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_local_progress_records(
    queue: core_queue.ClioCoreQueue,
    job_id: str,
    *,
    max_records: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Read a complete bounded progress snapshot or fail before using partial evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        page, next_cursor, total = queue.list_progress_page(
            job_id,
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
        )
        expected_total = _validate_complete_page(
            label=f"progress for {job_id}",
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
            max_records=max_records,
        )
        records.extend(item.model_dump(mode="json") for item in page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"progress for {job_id} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_remote_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
    max_records: int = MAX_INTERNAL_COLLECTION_RECORDS,
    deadline: float | None = None,
) -> list[dict[str, Any]]:
    """Drain a remote paged CLI collection under an explicit completeness cap."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        payload = _json_output(
            cli_remote_worker_probe._run_remote_clio_before_deadline(
                definition,
                [
                    *command,
                    "--cursor",
                    str(cursor),
                    "--limit",
                    str(MAX_RESPONSE_PAGE_RECORDS),
                ],
                deadline=deadline,
            ),
            label,
        )
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise RelayError(f"{label} did not return a {record_key} array")
        page: list[dict[str, Any]] = []
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise RelayError(f"{label} returned a non-object {record_key} entry")
            page.append(
                {str(key): value for key, value in cast(dict[object, object], item).items()}
            )
        total = payload.get("total")
        returned_cursor = payload.get("cursor")
        returned_limit = payload.get("limit")
        next_cursor = payload.get("next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise RelayError(f"{label} returned an invalid total")
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise RelayError(f"{label} returned inconsistent page metadata")
        if next_cursor is not None and (
            isinstance(next_cursor, bool) or not isinstance(next_cursor, int)
        ):
            raise RelayError(f"{label} returned an invalid next_cursor")
        expected_total = _validate_complete_page(
            label=label,
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
            max_records=max_records,
        )
        records.extend(page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"{label} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_remote_source_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
    max_source_positions: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Drain filtered global source windows while bounding every durable position."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        payload = _json_output(
            remote_cli.run_remote_clio(
                definition,
                [
                    *command,
                    "--cursor",
                    str(cursor),
                    "--limit",
                    str(MAX_RESPONSE_PAGE_RECORDS),
                ],
            ),
            label,
        )
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise RelayError(f"{label} did not return a {record_key} array")
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise RelayError(f"{label} returned a non-object {record_key} entry")
            records.append(
                {str(key): value for key, value in cast(dict[object, object], item).items()}
            )
        total = payload.get("source_total")
        returned_cursor = payload.get("source_cursor")
        returned_limit = payload.get("source_limit")
        next_cursor = payload.get("source_next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise RelayError(f"{label} returned an invalid total")
        if total > max_source_positions:
            raise RelayError(f"{label} exceeds the bounded source limit {max_source_positions}")
        if expected_total is not None and total != expected_total:
            raise RelayError(f"{label} changed during bounded discovery")
        expected_total = total
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise RelayError(f"{label} returned inconsistent page metadata")
        if next_cursor is None:
            return records
        if (
            isinstance(next_cursor, bool)
            or not isinstance(next_cursor, int)
            or next_cursor <= cursor
            or next_cursor > total
        ):
            raise RelayError(f"{label} returned an invalid next_cursor")
        cursor = next_cursor


def _validate_complete_page(
    *,
    label: str,
    cursor: int,
    page_count: int,
    next_cursor: int | None,
    total: int,
    expected_total: int | None,
    collected_count: int,
    max_records: int,
) -> int:
    """Validate a page chain before it can be treated as complete evidence."""
    if max_records < 1:
        raise ValueError("max_records must be positive")
    if total > max_records:
        raise RelayError(f"{label} exceeds the bounded completeness limit {max_records}")
    if expected_total is not None and total != expected_total:
        raise RelayError(f"{label} changed during bounded discovery")
    if collected_count + page_count > total:
        raise RelayError(f"{label} returned more records than its total")
    expected_next = cursor + page_count
    if next_cursor is not None and (
        page_count == 0 or next_cursor != expected_next or next_cursor > total
    ):
        raise RelayError(f"{label} returned a non-contiguous page cursor")
    if next_cursor is None and collected_count + page_count != total:
        raise RelayError(f"{label} ended before its declared total")
    return total

"""Remote owned-active-job listing, over the held channel when live
(iowarp/clio-relay#179 dial burn-down; extracted from ``cli_owned_relay_
jobs.py`` to stay under its file-size ratchet, #775).

Reaches ``cli_owned_relay_jobs``'s private ``_OwnedRelayJob``/
``_owned_relay_job``/``_relay_job_needs_cleanup``/``_job_is_owned_by_
session`` by module-attribute (the established cross-module pattern this
codebase already uses throughout, e.g. ``cli_scheduler.py`` reaching
``cli._require_cluster``). This is a one-directional dependency, not
circular: ``cli_owned_relay_jobs.py`` does not import this module -- its
external callers (``cli_session.py``, ``cli_session_teardown_jobs.py``)
reach this module directly instead of through a forwarder, per this
codebase's own "moved caller" precedent (``scripts/check_file_size.py``'s
ratchet history).
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from typing import cast

import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination
import clio_relay.remote_channel_dispatch as remote_channel_dispatch
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS


def list_remote_owned_active_cluster_jobs(
    definition: ClusterDefinition,
    cluster: str,
    *,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    include_terminal: bool = False,
) -> list[cli_owned_relay_jobs._OwnedRelayJob]:
    """List active jobs (clio-relay#179: rides GET /queue+tasks when live)."""
    connection = remote_channel_dispatch.live_matching_connection(
        definition=definition,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    )
    if connection is not None:
        return remote_channel_dispatch.build_owned_jobs_over_channel(
            connection,
            cluster=cluster,
            include_terminal=include_terminal,
            build=lambda job_document, task_documents: cli_owned_relay_jobs._owned_relay_job(
                job_document, task_documents, scheduler_provider=definition.scheduler_provider
            ),
            needs_inclusion=cli_owned_relay_jobs._relay_job_needs_cleanup,
        )
    remote_channel_dispatch.record_per_operation_ssh_fallback(
        operation="list_remote_owned_active_cluster_jobs",
        cluster=cluster,
        detail="no live owned-session channel matches this exact identity",
    )
    owned: list[cli_owned_relay_jobs._OwnedRelayJob] = []
    membership_generations = [owner_session_generation_id]
    for membership_generation in membership_generations:
        cursor: str | None = None
        expected_total: int | None = None
        processed_source = 0
        while True:
            command = [
                "queue",
                "owner-jobs",
                "--cluster",
                cluster,
                "--owner-session-id",
                owner_session_id,
                "--limit",
                str(MAX_RESPONSE_PAGE_RECORDS),
            ]
            if membership_generation is not None:
                command.extend(["--owner-session-generation-id", membership_generation])
            if include_terminal:
                command.append("--include-terminal")
            if cursor is not None:
                command.extend(["--cursor", cursor])
            payload = cli_remote_collection_pagination._json_output(
                remote_cli.run_remote_clio(definition, command),
                f"remote owner-session jobs for {cluster}",
            )
            raw_jobs = payload.get("jobs")
            if not isinstance(raw_jobs, list):
                raise RelayError("remote owner-session membership returned no jobs array")
            total = payload.get("source_total")
            if isinstance(total, bool) or not isinstance(total, int) or total < 0:
                raise RelayError("remote owner-session membership returned an invalid total")
            if total > cli_remote_collection_pagination.MAX_INTERNAL_COLLECTION_RECORDS:
                raise RelayError(
                    "remote owner-session membership exceeds the bounded source limit "
                    f"{cli_remote_collection_pagination.MAX_INTERNAL_COLLECTION_RECORDS}"
                )
            if expected_total is not None and total != expected_total:
                raise RelayError("remote owner-session membership changed during discovery")
            expected_total = total
            source_window_count = payload.get("source_window_count")
            if (
                isinstance(source_window_count, bool)
                or not isinstance(source_window_count, int)
                or source_window_count < 0
                or source_window_count > MAX_RESPONSE_PAGE_RECORDS
            ):
                raise RelayError("remote owner-session membership returned an invalid source count")
            processed_source += source_window_count
            for raw_job in cast(list[object], raw_jobs):
                if not isinstance(raw_job, dict):
                    raise RelayError("remote owner-session membership returned a non-object job")
                job_document = {
                    str(key): value for key, value in cast(dict[object, object], raw_job).items()
                }
                if not cli_owned_relay_jobs._job_is_owned_by_session(
                    job_document,
                    owner_session_id,
                    owner_session_generation_id=owner_session_generation_id,
                ):
                    raise RelayError("remote owner-session membership target identity mismatch")
                job_id = job_document.get("job_id")
                if not isinstance(job_id, str):
                    raise RelayError("remote owner-session membership omitted job_id")
                task_documents = cli_remote_collection_pagination._complete_remote_collection(
                    definition,
                    ["job", "tasks", job_id],
                    record_key="tasks",
                    label=f"remote owner-session tasks for {job_id}",
                )
                candidate = cli_owned_relay_jobs._owned_relay_job(
                    job_document,
                    task_documents,
                    scheduler_provider=definition.scheduler_provider,
                )
                if include_terminal or cli_owned_relay_jobs._relay_job_needs_cleanup(candidate):
                    owned.append(candidate)
            next_cursor = payload.get("source_next_cursor")
            if next_cursor is None:
                if processed_source != total:
                    raise RelayError(
                        "remote owner-session membership ended before its declared total"
                    )
                break
            if not isinstance(next_cursor, str) or (cursor is not None and next_cursor <= cursor):
                raise RelayError("remote owner-session membership returned an invalid cursor")
            cursor = next_cursor
    return owned


__all__ = ["list_remote_owned_active_cluster_jobs"]

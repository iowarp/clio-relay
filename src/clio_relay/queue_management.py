"""Queue management operations shared by CLI, HTTP, and MCP surfaces.

This module is a thin re-export facade (clio-relay#231-style owner-module
decomposition). The implementation lives in single-concern owner modules;
this file exists only so every caller's ``from clio_relay.queue_management
import ...`` keeps working unchanged:

* ``queue_diagnosis_constants`` -- shared limit/policy constants and the
  bound/cluster validators every entry point below applies first.
* ``queue_worker_capacity`` -- worker process-generation selection and
  kind/workload/control-query concurrency-policy parsing.
* ``queue_admission_snapshot`` -- the bounded ``_AdmissionSnapshot`` capacity
  read (fresh workers + durable leases, cross-checked against the lease
  index).
* ``queue_listing`` -- raw cluster submission order and job-listing (page
  the queue).
* ``queue_admission_simulation`` -- the next-job admission simulation that
  turns raw submission order plus the admission snapshot into "admissible
  now, or blocked by whom".
* ``queue_diagnosis`` -- per-job and per-cluster progress diagnosis built on
  the two modules above.
* ``queue_stale_recovery`` -- stale-job discovery, bounded recovery, and
  explicit job cancellation.
* ``queue_worker_status`` -- registered worker capacity and current lease
  reporting.

Import from those modules directly in new code; this facade is for existing
call sites and is not itself where new queue-management logic should land.
"""

from __future__ import annotations

from clio_relay.queue_diagnosis import diagnose_job, diagnose_queue
from clio_relay.queue_diagnosis_constants import (
    ACTIVE_STATES,
    DEFAULT_RESULT_LIMIT,
    DEFAULT_SCAN_LIMIT,
    DEFAULT_STALE_AFTER_SECONDS,
    DEFAULT_STALE_SCAN_LIMIT,
    DEFAULT_WORKER_FRESH_SECONDS,
    MAX_RESULT_LIMIT,
    MAX_SCAN_LIMIT,
    QueueCancelPolicy,
)
from clio_relay.queue_listing import list_queue_jobs
from clio_relay.queue_stale_recovery import (
    cancel_queue_job,
    cleanup_stale_jobs,
    discover_stale_jobs,
)
from clio_relay.queue_worker_status import worker_status

__all__ = [
    "ACTIVE_STATES",
    "DEFAULT_RESULT_LIMIT",
    "DEFAULT_SCAN_LIMIT",
    "DEFAULT_STALE_AFTER_SECONDS",
    "DEFAULT_STALE_SCAN_LIMIT",
    "DEFAULT_WORKER_FRESH_SECONDS",
    "MAX_RESULT_LIMIT",
    "MAX_SCAN_LIMIT",
    "QueueCancelPolicy",
    "cancel_queue_job",
    "cleanup_stale_jobs",
    "diagnose_job",
    "diagnose_queue",
    "discover_stale_jobs",
    "list_queue_jobs",
    "worker_status",
]

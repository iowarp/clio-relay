"""Canonical live validation for production relay queue management.

This module is now an assembly/facade only (iowarp/clio-relay#231-style
split, ground rule 5: single owner per concern, no god-files). Every real
concern moved verbatim to its own owner module:

* ``live_validation_constants.py`` -- tuning constants (kind-cap, timeouts,
  the bounded command's marker schema).
* ``live_validation_support.py`` -- generic assertion/coercion/evidence
  primitives shared by every other owner module.
* ``live_validation_process.py`` -- worker process/task/lease discovery,
  the ``_WorkerProcessObservation`` evidence record, and cancellation
  waiting.
* ``live_validation_capacity.py`` -- fleet capacity/containment/heartbeat
  checks and the durable lease-capacity audit.
* ``live_validation_jobs.py`` -- per-job validation checks (bounded
  listing, stale diagnosis/cleanup, the bounded validation command job
  itself) and cleanup-evidence recording.
* ``live_validation_cleanup.py`` -- end-of-run resource cleanup and the
  shared scheduler-phase poll loop.
* ``live_validation_orchestrator.py`` -- ``run_queue_management_validation``,
  the one public entry point, composing all of the above.

Every name this module used to define at top level is re-imported here
under its original name (the ``as`` form marks each as an intentional
public re-export so lint does not flag it as unused), so every existing
``from clio_relay.queue_validation import X`` caller and every
``clio_relay.queue_validation.X`` qualified/monkeypatch access -- including
``cli_queue_maintenance.py``'s ``queue_validation.run_queue_management_
validation`` call and this package's own test suite -- keeps resolving
unchanged. A pure move, not a behavior change.
"""

from __future__ import annotations

from clio_relay.live_validation_capacity import (
    _controlled_capacity as _controlled_capacity,
)
from clio_relay.live_validation_capacity import (
    _controlled_process_containment as _controlled_process_containment,
)
from clio_relay.live_validation_capacity import (
    _require_quiet_validation_queue as _require_quiet_validation_queue,
)
from clio_relay.live_validation_capacity import (
    _validate_lease_capacity_audit as _validate_lease_capacity_audit,
)
from clio_relay.live_validation_capacity import (
    _worker_heartbeat_snapshot as _worker_heartbeat_snapshot,
)
from clio_relay.live_validation_cleanup import (
    _cleanup_validation_resources as _cleanup_validation_resources,
)
from clio_relay.live_validation_cleanup import (
    _wait_for_scheduler_phase as _wait_for_scheduler_phase,
)
from clio_relay.live_validation_constants import (
    MAX_VALIDATION_POLL_SECONDS as MAX_VALIDATION_POLL_SECONDS,
)
from clio_relay.live_validation_constants import (
    MAX_VALIDATION_SCHEDULER_TIMEOUT_SECONDS as MAX_VALIDATION_SCHEDULER_TIMEOUT_SECONDS,
)
from clio_relay.live_validation_constants import (
    PROCESS_DISCOVERY_TIMEOUT_SECONDS as PROCESS_DISCOVERY_TIMEOUT_SECONDS,
)
from clio_relay.live_validation_constants import (
    VALIDATION_COMMAND_SECONDS as VALIDATION_COMMAND_SECONDS,
)
from clio_relay.live_validation_constants import (
    VALIDATION_KIND_LIMIT as VALIDATION_KIND_LIMIT,
)
from clio_relay.live_validation_constants import (
    VALIDATION_MARKER_SCHEMA as VALIDATION_MARKER_SCHEMA,
)
from clio_relay.live_validation_constants import (
    VALIDATION_MINIMUM_TOTAL_CONCURRENCY as VALIDATION_MINIMUM_TOTAL_CONCURRENCY,
)
from clio_relay.live_validation_jobs import (
    _cancel_optional_anchor as _cancel_optional_anchor,
)
from clio_relay.live_validation_jobs import (
    _cancel_queued_validation_job as _cancel_queued_validation_job,
)
from clio_relay.live_validation_jobs import (
    _listed_job_ids as _listed_job_ids,
)
from clio_relay.live_validation_jobs import (
    _plan_for_job as _plan_for_job,
)
from clio_relay.live_validation_jobs import (
    _record_job_cleanup as _record_job_cleanup,
)
from clio_relay.live_validation_jobs import (
    _validate_bounded_listing as _validate_bounded_listing,
)
from clio_relay.live_validation_jobs import (
    _validate_specific_diagnosis as _validate_specific_diagnosis,
)
from clio_relay.live_validation_jobs import (
    _validate_stale_cleanup as _validate_stale_cleanup,
)
from clio_relay.live_validation_jobs import (
    _validation_execution_job as _validation_execution_job,
)
from clio_relay.live_validation_orchestrator import (
    run_queue_management_validation as run_queue_management_validation,
)
from clio_relay.live_validation_process import (
    _complete_cluster_endpoints as _complete_cluster_endpoints,
)
from clio_relay.live_validation_process import (
    _complete_job_leases as _complete_job_leases,
)
from clio_relay.live_validation_process import (
    _complete_job_tasks as _complete_job_tasks,
)
from clio_relay.live_validation_process import (
    _iter_job_events as _iter_job_events,
)
from clio_relay.live_validation_process import (
    _latest_cancel_request as _latest_cancel_request,
)
from clio_relay.live_validation_process import (
    _process_exists as _process_exists,
)
from clio_relay.live_validation_process import (
    _process_markers as _process_markers,
)
from clio_relay.live_validation_process import (
    _scheduler_cancel_events as _scheduler_cancel_events,
)
from clio_relay.live_validation_process import (
    _wait_for_worker_admission_cycle as _wait_for_worker_admission_cycle,
)
from clio_relay.live_validation_process import (
    _wait_for_worker_cancellation as _wait_for_worker_cancellation,
)
from clio_relay.live_validation_process import (
    _wait_for_worker_process_started as _wait_for_worker_process_started,
)
from clio_relay.live_validation_process import (
    _WorkerProcessObservation as _WorkerProcessObservation,
)
from clio_relay.live_validation_support import (
    _combined_error as _combined_error,
)
from clio_relay.live_validation_support import (
    _evidence as _evidence,
)
from clio_relay.live_validation_support import (
    _list as _list,
)
from clio_relay.live_validation_support import (
    _mapping as _mapping,
)
from clio_relay.live_validation_support import (
    _require as _require,
)
from clio_relay.live_validation_support import (
    _require_cluster as _require_cluster,
)
from clio_relay.live_validation_support import (
    _validate_options as _validate_options,
)

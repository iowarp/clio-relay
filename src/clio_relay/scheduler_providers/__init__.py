"""Scheduler provider boundary for cluster job status and cancellation.

This package is a thin facade: every name below is implemented in an owner
module (``protocols.py``, ``external.py``, ``slurm_provider.py``,
``slurm_connector.py``, ``slurm_connector_launcher.py``, ``slurm_status.py``,
``validation.py``, ``command.py``, ``registry.py``, ``constants.py``) and
re-exported here so ``clio_relay.scheduler_providers`` keeps its exact
original import surface -- no caller anywhere in the repository needs to
change its import statement.

Two things about this facade are load-bearing, not decorative:

* ``import subprocess`` -- several tests monkeypatch
  ``"clio_relay.scheduler_providers.subprocess.run"`` /
  ``".subprocess.Popen"`` by dotted string path. ``subprocess`` is a single
  module object shared by every importer (``sys.modules["subprocess"]``), so
  this import only needs to exist for that dotted path to resolve; the
  actual patch lands on the shared module every owner file's own
  ``subprocess.run(...)`` call already reads from.
* ``CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS``,
  ``CONNECTOR_STEP_FAILED_RECONCILIATION_OBSERVATIONS``, and
  ``_register_connector_launcher_for_reaping`` -- tests monkeypatch these
  three names directly on this facade module. ``slurm_connector.py`` reads
  them back off ``clio_relay.scheduler_providers`` (this module) at call
  time instead of importing them by value, specifically so those patches
  are observed. See that module's docstring for the mechanism.
"""

from __future__ import annotations

import subprocess

from .command import (
    _run_scheduler_command,
    _scheduler_command_error,
    _slurm_job_absent_from_active_queue,
)
from .constants import (
    CONNECTOR_LAUNCHER_DIAGNOSTIC_BYTES,
    CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS,
    CONNECTOR_STEP_FAILED_RECONCILIATION_OBSERVATIONS,
    CONNECTOR_STEP_REGISTRATION_POLL_SECONDS,
    CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS,
    SACCT_FIELDS,
    SCHEDULER_ALLOCATED_CHECK_ID,
    SCHEDULER_COMMAND_TIMEOUT_SECONDS,
    SCHEDULER_COMPLETED_CHECK_ID,
    SCHEDULER_PENDING_CHECK_ID,
    SCHEDULER_RECONCILIATION_MAX_AGE,
    SCHEDULER_RECONCILIATION_TIME_TOLERANCE,
    SCHEDULER_RUNNING_CHECK_ID,
    SCHEDULER_RUNTIME_METADATA_CHECK_ID,
    SQUEUE_FIELDS,
)
from .external import ExternalSchedulerProvider
from .protocols import (
    SchedulerAllocationConnectorProvider,
    SchedulerProvider,
    SchedulerReconciliationProvider,
    SchedulerValidationProvider,
)
from .registry import (
    _PROVIDER_FACTORIES,
    SchedulerProviderFactory,
    allocation_connector_provider_for_scheduler,
    provider_for_scheduler,
    reconciliation_provider_for_scheduler,
    register_scheduler_provider,
    validation_provider_for_scheduler,
)
from .slurm_connector import (
    _CONNECTOR_STEP_MARKER,
    _SLURM_ALLOCATION_JOB_ID,
    _validate_connector_command,
    _validate_connector_output_path,
    _validate_connector_placement_host,
    _validate_connector_step_id,
    _validate_connector_step_marker,
    _validate_slurm_allocation_job_id,
)
from .slurm_connector_launcher import (
    _read_connector_launcher_diagnostic,
    _reap_connector_launchers,
    _register_connector_launcher_for_reaping,
    _terminate_connector_launcher,
)
from .slurm_provider import SlurmSchedulerProvider
from .slurm_status import (
    _empty_to_none,
    _optional_int,
    _parse_scontrol_record,
    _phase_from_slurm_state,
    _sort_time,
    _split_row,
    _status_from_sacct_row,
    _status_from_scontrol_record,
    _status_from_squeue_row,
    _with_queue_position,
)
from .validation import (
    _SCHEDULER_JOB_ID,
    _VALIDATION_JOB_NAME,
    _normalize_provider_name,
    _parse_slurm_reconciliation_time,
    _validate_reconciliation_marker,
    _validate_reconciliation_time,
    _validate_scheduler_job_id,
    _validate_scheduler_user,
    _validate_validation_job_name,
)

__all__ = [
    "CONNECTOR_LAUNCHER_DIAGNOSTIC_BYTES",
    "CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS",
    "CONNECTOR_STEP_FAILED_RECONCILIATION_OBSERVATIONS",
    "CONNECTOR_STEP_REGISTRATION_POLL_SECONDS",
    "CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS",
    "SACCT_FIELDS",
    "SCHEDULER_ALLOCATED_CHECK_ID",
    "SCHEDULER_COMMAND_TIMEOUT_SECONDS",
    "SCHEDULER_COMPLETED_CHECK_ID",
    "SCHEDULER_PENDING_CHECK_ID",
    "SCHEDULER_RECONCILIATION_MAX_AGE",
    "SCHEDULER_RECONCILIATION_TIME_TOLERANCE",
    "SCHEDULER_RUNNING_CHECK_ID",
    "SCHEDULER_RUNTIME_METADATA_CHECK_ID",
    "SQUEUE_FIELDS",
    "ExternalSchedulerProvider",
    "SchedulerAllocationConnectorProvider",
    "SchedulerProvider",
    "SchedulerProviderFactory",
    "SchedulerReconciliationProvider",
    "SchedulerValidationProvider",
    "SlurmSchedulerProvider",
    "_CONNECTOR_STEP_MARKER",
    "_PROVIDER_FACTORIES",
    "_SCHEDULER_JOB_ID",
    "_SLURM_ALLOCATION_JOB_ID",
    "_VALIDATION_JOB_NAME",
    "_empty_to_none",
    "_normalize_provider_name",
    "_optional_int",
    "_parse_scontrol_record",
    "_parse_slurm_reconciliation_time",
    "_phase_from_slurm_state",
    "_reap_connector_launchers",
    "_read_connector_launcher_diagnostic",
    "_register_connector_launcher_for_reaping",
    "_run_scheduler_command",
    "_scheduler_command_error",
    "_slurm_job_absent_from_active_queue",
    "_sort_time",
    "_split_row",
    "_status_from_sacct_row",
    "_status_from_scontrol_record",
    "_status_from_squeue_row",
    "_terminate_connector_launcher",
    "_validate_connector_command",
    "_validate_connector_output_path",
    "_validate_connector_placement_host",
    "_validate_connector_step_id",
    "_validate_connector_step_marker",
    "_validate_reconciliation_marker",
    "_validate_reconciliation_time",
    "_validate_scheduler_job_id",
    "_validate_scheduler_user",
    "_validate_slurm_allocation_job_id",
    "_validate_validation_job_name",
    "_with_queue_position",
    "allocation_connector_provider_for_scheduler",
    "provider_for_scheduler",
    "reconciliation_provider_for_scheduler",
    "register_scheduler_provider",
    "subprocess",
    "validation_provider_for_scheduler",
]

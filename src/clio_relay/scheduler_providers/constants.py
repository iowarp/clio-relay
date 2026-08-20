"""Module-level constants for the scheduler provider boundary.

Kept as a dependency-free leaf so every owner module (and the package
facade, ``clio_relay/scheduler_providers/__init__.py``) can import these
values without creating an import cycle. Several of these names are
monkeypatched directly on the facade by tests (for example
``CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS``); callers that must observe
such a patch read the value back off the facade module at call time rather
than importing it by value -- see ``slurm_connector.py``.
"""

from __future__ import annotations

from datetime import timedelta

SQUEUE_FIELDS = "%i|%T|%R|%P|%q|%u|%D|%C|%m|%V|%S|%M|%l"
SACCT_FIELDS = "JobIDRaw,State,Partition,QOS,Submit,Start,Elapsed,NNodes,NCPUS,ReqMem"
SCHEDULER_PENDING_CHECK_ID = "scheduler.pending"
SCHEDULER_ALLOCATED_CHECK_ID = "scheduler.allocated"
SCHEDULER_RUNNING_CHECK_ID = "scheduler.running"
SCHEDULER_COMPLETED_CHECK_ID = "scheduler.completed"
SCHEDULER_RUNTIME_METADATA_CHECK_ID = "scheduler.structured-metadata"
SCHEDULER_COMMAND_TIMEOUT_SECONDS = 15.0
CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS = 15.0
CONNECTOR_STEP_REGISTRATION_POLL_SECONDS = 0.2
CONNECTOR_LAUNCHER_DIAGNOSTIC_BYTES = 16 * 1024
CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS = 15.0
CONNECTOR_STEP_FAILED_RECONCILIATION_OBSERVATIONS = 3
SCHEDULER_RECONCILIATION_MAX_AGE = timedelta(days=7)
SCHEDULER_RECONCILIATION_TIME_TOLERANCE = timedelta(seconds=5)

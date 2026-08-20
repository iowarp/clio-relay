"""Tuning constants for the live queue-management validation fixture.

Single owner for every bound the ``queue_validation`` live-validation flow
enforces: the required JARVIS kind concurrency and minimum worker fleet size
the fixture proves against, the bounded validation command's own sleep/
timeout window, the process-observation marker schema tag, and the
wait/poll ceilings the fixture itself is allowed to request. Every sibling
``live_validation_*`` owner module imports from here rather than keeping its
own copy, so the numbers move together.
"""

from __future__ import annotations

VALIDATION_KIND_LIMIT = 2
VALIDATION_MINIMUM_TOTAL_CONCURRENCY = 3
VALIDATION_COMMAND_SECONDS = 300
VALIDATION_MARKER_SCHEMA = "clio-relay.queue-validation-process.v1"
MAX_VALIDATION_SCHEDULER_TIMEOUT_SECONDS = 600.0
MAX_VALIDATION_POLL_SECONDS = 10.0
PROCESS_DISCOVERY_TIMEOUT_SECONDS = 5.0

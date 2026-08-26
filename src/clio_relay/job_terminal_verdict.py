"""Render ``_run_job_impl``'s terminal FAILED ``message``/``error`` text.

Owner module for the priority-ordered typed-reason rendering
``endpoint_job_execution.py`` used to carry inline (adversarial-review item
2: that module sits at its own line-count ratchet ceiling, #774/#775, and a
NEW god-file exemption is not the fix -- the guard's own docstring forbids
that; extracting the three chains below is). All three functions take the
SAME typed-reason candidates ``_run_job_impl`` already resolves, in the
SAME fixed priority order established across #265/#266/#183/#248: a JARVIS
dispatch refusal, then a #266 execution-watch failure, then Ruling A's
``application_verdict_failure``, then a #265 ``outputs_missing_failure``
(``execution_watch.ExecutionOutcome``'s GATED field -- populated ONLY when
``_outputs_missing_forces_failure`` says so, never the raw, unconditional
``outputs_missing`` signal; adversarial-review fix: threading the raw
signal in here instead let a present-but-non-forcing ``no_outputs_declared``
hijack an unrelated real failure, rendering "completed but declared zero
outputs" and suppressing the #183/#248 tier below it entirely), then a
#183/#248 generic MCP dispatch failure, and only then the last-resort bare
exit code this whole campaign exists to stop being the ONLY thing a caller
ever sees.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from clio_relay import execution_watch
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.mcp_call_result_error import mcp_call_dispatch_failure_text

if TYPE_CHECKING:
    from clio_relay.jarvis_dispatch_failure import JarvisDispatchRefusal


def terminal_failure_metadata(
    *,
    effective_returncode: int,
    dispatch_refusal: JarvisDispatchRefusal | None,
    watch_failure: dict[str, object] | None,
    application_verdict_failure: dict[str, object] | None,
    outputs_missing_signal: dict[str, object] | None,
    mcp_dispatch_failure: dict[str, object] | None,
) -> dict[str, object]:
    """Return the ``update_task_state`` FAILED ``metadata`` fold.

    ``outputs_missing_signal`` is deliberately the RAW, unconditional
    ``ExecutionOutcome.outputs_missing`` (never the gated ``outputs_
    missing_failure``): Ruling B's signal must reach the durable record
    whenever present, regardless of whether it drove this failure -- the
    same unconditional fold the SUCCESS branch already does.
    """
    metadata: dict[str, object] = {"returncode": effective_returncode}
    if dispatch_refusal is not None:
        metadata["jarvis_dispatch_refusal"] = dispatch_refusal.as_payload()
    if watch_failure is not None:
        metadata["execution_watch_failure"] = watch_failure
    if application_verdict_failure is not None:
        metadata["application_verdict_failure"] = application_verdict_failure
    if outputs_missing_signal is not None:
        metadata["execution_outputs_missing"] = outputs_missing_signal
    if mcp_dispatch_failure is not None:
        metadata["mcp_dispatch_failure"] = mcp_dispatch_failure
    return metadata


def terminal_failure_message(
    *,
    dispatch_refusal: JarvisDispatchRefusal | None,
    watch_failure: dict[str, object] | None,
    application_verdict_failure: dict[str, object] | None,
    outputs_missing_failure: dict[str, object] | None,
    endpoint_mcp_call: bool,
) -> str:
    """Return the one-line ``update_job_state`` ``message`` for a FAILED job."""
    if dispatch_refusal is not None:
        return "JARVIS run failed"
    if watch_failure is not None:
        return "JARVIS execution ended in failure"
    if application_verdict_failure is not None:
        # Ruling A: JARVIS reported the launcher exited cleanly, but its own
        # returncode field disagrees -- never render this as a bare exit
        # code just because the SCHEDULER-level watch called it succeeded.
        reason = application_verdict_failure.get("reason")
        return f"JARVIS execution application verdict failed: {reason}"
    if outputs_missing_failure is not None:
        # clio-relay#265 owner ruling: "producing the declared outputs is
        # PART of what completed means" -- an execution JARVIS itself
        # reported terminal, but whose declared outputs are missing or
        # empty (declared_outputs_missing; no_outputs_declared never
        # reaches here, Ruling B), is FAILED here too.
        return "JARVIS execution completed but declared outputs are missing or empty"
    if endpoint_mcp_call:
        return "Endpoint MCP operation failed"
    return "JARVIS-CD run failed"


def terminal_failure_error_text(
    *,
    dispatch_refusal: JarvisDispatchRefusal | None,
    watch_failure: dict[str, object] | None,
    application_verdict_failure: dict[str, object] | None,
    outputs_missing_failure: dict[str, object] | None,
    mcp_dispatch_failure: dict[str, object] | None,
    effective_returncode: int,
) -> str | None:
    """Return the bounded ``update_job_state`` ``error`` text for a FAILED job."""
    if dispatch_refusal is not None:
        return bounded_error_detail(dispatch_refusal.as_error_detail())
    if watch_failure is not None:
        return bounded_error_detail(execution_watch.execution_watch_error_text(watch_failure))
    if application_verdict_failure is not None:
        reason = application_verdict_failure.get("reason")
        returncode = application_verdict_failure.get("application_returncode")
        return bounded_error_detail(
            f"JARVIS execution application verdict failed ({reason}): "
            f"application_returncode={returncode}"
        )
    if outputs_missing_failure is not None:
        return bounded_error_detail(
            execution_watch.execution_outputs_missing_error_text(outputs_missing_failure)
        )
    if mcp_dispatch_failure is not None:
        return bounded_error_detail(mcp_call_dispatch_failure_text(mcp_dispatch_failure))
    return f"exit code {effective_returncode}"

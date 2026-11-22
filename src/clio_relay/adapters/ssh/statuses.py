"""Reading a job's state on an ssh site: Slurm accounting, a recorded end, a detached job."""

from __future__ import annotations

import contextlib

from clio_relay.jobs.handles import JobState, JobStatus


def _one(sections: dict[str, list[str]], key: str) -> str | None:
    values = [value.strip() for value in sections.get(key, []) if value.strip()]
    return values[0] if values else None


def _int(value: str | None) -> int | None:
    if value is None:
        return None
    with contextlib.suppress(ValueError):
        return int(value)
    return None


_ACCOUNTED_FAILURES = (
    "FAILED",
    "TIMEOUT",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "BOOT_FAIL",
    "DEADLINE",
    "PREEMPTED",
)


def accounted_status(lines: list[str]) -> JobStatus | None:
    """Read Slurm accounting (``State|ExitCode``) for a job the queue has forgotten."""
    for line in lines:
        state, _, code = line.strip().partition("|")
        if not state:
            continue
        native = state.split()[0]
        exit_code = _int(code.split(":", 1)[0]) if code else None
        if native == "COMPLETED":
            return JobStatus(
                state=JobState.COMPLETED if exit_code in (0, None) else JobState.FAILED,
                exit_code=exit_code,
                native_state=native,
            )
        if native == "CANCELLED":
            return JobStatus(state=JobState.CANCELLED, exit_code=exit_code, native_state=native)
        if native in _ACCOUNTED_FAILURES:
            return JobStatus(
                state=JobState.FAILED,
                exit_code=exit_code,
                native_state=native,
                message=f"Slurm accounting says {state.lower()}",
            )
        return None
    return None


def stored_final(lines: list[str]) -> JobStatus:
    """Read the end the job directory recorded."""
    text = "\n".join(lines).strip()
    try:
        return JobStatus.model_validate_json(text)
    except ValueError:
        return JobStatus(state=JobState.FAILED, native_state="final", message=text[:500] or None)


def detached_status(sections: dict[str, list[str]]) -> JobStatus:
    """Read a detached (supervised) job's state from its status sections."""
    exit_code = _int(_one(sections, "exit"))
    cancelled = "cancel" in sections
    if exit_code is not None:
        if cancelled and exit_code != 0:
            return JobStatus(state=JobState.CANCELLED, exit_code=exit_code, native_state="exited")
        state = JobState.COMPLETED if exit_code == 0 else JobState.FAILED
        return JobStatus(state=state, exit_code=exit_code, native_state="exited")
    if "alive" in sections:
        if "started" in sections:
            return JobStatus(state=JobState.RUNNING, native_state="running")
        return JobStatus(state=JobState.QUEUED, native_state="starting")
    if cancelled:
        return JobStatus(state=JobState.CANCELLED, native_state="gone")
    return JobStatus(
        state=JobState.FAILED,
        native_state="lost",
        message="the supervisor is gone without an exit code; the host may have restarted",
    )

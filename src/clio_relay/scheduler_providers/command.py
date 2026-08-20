"""Bounded subprocess execution boundary shared by every scheduler command."""

from __future__ import annotations

import subprocess

from clio_relay.errors import RelayError

from .constants import SCHEDULER_COMMAND_TIMEOUT_SECONDS


def _run_scheduler_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
            timeout=SCHEDULER_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(
            f"scheduler provider command timed out after "
            f"{SCHEDULER_COMMAND_TIMEOUT_SECONDS:g}s: {command[0]}"
        ) from exc
    except OSError as exc:
        raise RelayError(f"scheduler provider command failed: {command[0]}: {exc}") from exc


def _slurm_job_absent_from_active_queue(
    result: subprocess.CompletedProcess[str],
) -> bool:
    """Recognize SLURM's nonzero response for a job no longer visible to ``squeue``."""
    return (
        result.returncode != 0
        and not result.stdout.strip()
        and "invalid job id specified" in result.stderr.casefold()
    )


def _scheduler_command_error(
    executable: str,
    result: subprocess.CompletedProcess[str],
) -> RelayError:
    detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
    return RelayError(f"scheduler provider command failed: {executable}: {detail}")

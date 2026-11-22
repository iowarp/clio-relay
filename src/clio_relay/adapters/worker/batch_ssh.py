"""A plain ``ssh`` with ``BatchMode=yes`` that runs one script from stdin.

The worker adapter uses it when its worker is not there to ask: cleaning the
site after the worker stopped, or listing relay's jobs on the site's scheduler.
"""

from __future__ import annotations

import asyncio
import contextlib
import os

from clio_relay import programs
from clio_relay.jobs.refusals import Refusal, RefusalReason

CLEAN_TIMEOUT_S = 60.0


async def batch_ssh(argv: list[str], script: str, what: str, site_id: str) -> str:
    """Run ``script`` on ``argv``'s host and return its output; ``what`` names it in refusals."""
    program = programs.resolved(argv)
    if program is None:
        raise Refusal(
            RefusalReason.UNREACHABLE, programs.missing(argv[0]), site=site_id, command=argv[0]
        )
    try:
        process = await asyncio.create_subprocess_exec(
            *program,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
    except OSError as exc:
        raise Refusal(RefusalReason.UNREACHABLE, f"ssh did not start: {exc}", site=site_id) from exc
    try:
        out, err = await asyncio.wait_for(
            process.communicate(script.encode("utf-8")), CLEAN_TIMEOUT_S
        )
    except TimeoutError as exc:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        raise Refusal(
            RefusalReason.TIMEOUT, f"{what} took over {CLEAN_TIMEOUT_S:g} s", site=site_id
        ) from exc
    if process.returncode != 0:
        raise Refusal(
            RefusalReason.NEEDS_SIGN_IN,
            f"{what} needs an ssh that signs in without a prompt (exit {process.returncode}): "
            f"{err.decode('utf-8', 'replace')[-1000:]}",
            site=site_id,
        )
    return out.decode("utf-8", "replace")

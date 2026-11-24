"""Relay's own state on a site, which ``site clean`` deletes, and the runs kept out of it.

That state is the site's own deployment: the directories its cleaning adapter
names (:meth:`~clio_relay.adapters.base.CleansSite.clean_targets`), its configured
``remote_dir`` first, with the worker and the login hop under it. Never a home-wide
path: another relay home on the same account has its own ``remote_dir``.

A run's outputs and inputs are the person's. They never belong in relay's
deployment, so a run or serve whose job names a path there is refused before
anything is asked, with the site's outputs directory to use instead.
"""

from __future__ import annotations

from typing import Any

from clio_relay.adapters.base import CleansSite
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec


def relay_dir(adapter: Any) -> str | None:
    """Return the site's own relay directory (its ``remote_dir``), or None without one."""
    if not isinstance(adapter, CleansSite):
        return None
    found, _kept = adapter.clean_targets()
    return found[0] if found else None


def under(path: str, targets: list[str]) -> bool:
    """Say whether ``path``, absolute or ``~/`` on the site, is inside one of ``targets``."""
    for target in targets:
        if target.startswith("/"):
            root = target.rstrip("/")
            if path == root or path.startswith(root + "/"):
                return True
            continue
        # ``~/x`` or ``x`` is under the home, which relay does not know here.
        part = "/" + target.removeprefix("~").strip("/") + "/"
        if part != "//" and part in path + "/":
            return True
    return False


def job_paths(job: JobSpec) -> list[str]:
    """Return the site paths a job names: its packages' parameters, directory and streams."""
    named = [job.directory, job.stdin_path, job.stdout_path, job.stderr_path]
    for step in job.steps():
        named += [value for value in step.parameters.values() if isinstance(value, str)]
    return [
        path.strip()
        for path in named
        if path and (path.strip().startswith(("/", "~/")) or path.strip() == "~")
    ]


def refuse_relay_state(adapter: Any, job: JobSpec, site_id: str) -> None:
    """Refuse a job that names a path inside relay's own directories on the site."""
    if not isinstance(adapter, CleansSite):
        return
    found, _kept = adapter.clean_targets()
    targets = list(found)
    outputs = getattr(getattr(adapter, "config", None), "outputs_dir", None)
    if outputs and under(outputs if outputs.startswith(("/", "~")) else f"~/{outputs}", targets):
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            f"{site_id}'s outputs_dir {outputs} is inside relay's own directories "
            f"({', '.join(targets)}), which site clean deletes. Set it to a directory outside them",
            site=site_id,
            outputs_dir=outputs,
        )
    inside = [path for path in job_paths(job) if under(path, targets)]
    if not inside:
        return
    instead = f" Use the site's outputs directory, {outputs}, instead." if outputs else ""
    raise Refusal(
        RefusalReason.INVALID_SPEC,
        f"the job names {', '.join(inside)}, inside relay's own directories on {site_id} "
        f"({', '.join(targets)}), which site clean deletes. A run's inputs and outputs "
        f"belong outside them.{instead}",
        site=site_id,
        paths=inside,
        outputs_dir=outputs,
    )

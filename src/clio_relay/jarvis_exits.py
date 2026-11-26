"""The site script's typed exits, as refusals, for every adapter that runs it.

The site script (:data:`~clio_relay.jarvis_site_script.SITE_SCRIPT`) exits 69 when
relay's JARVIS is missing, 3 when JARVIS needs software installed or a choice,
4 when JARVIS refused to build outside an allocation, 5 when JARVIS refused the
pipeline and 6 when something JARVIS ran failed (its failure records). Its
``check`` mode (the light
``ppl install +check``) exits 0 with JARVIS's install report when JARVIS's
record says the package's software is all there. Its ``install`` mode exits 0
with no job when JARVIS answered on the submitting host (:func:`checked_here`)
or started its build there (:func:`login_build`).
"""

from __future__ import annotations

import json
import re
from typing import Any, cast

from clio_relay import install_failures, install_time, jarvis_report, jarvis_submit
from clio_relay.jobs.handles import JobHandle
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec


def refuse(
    exit_code: int | str | None,
    output: str,
    error: str,
    *,
    spec: JobSpec,
    site: str,
    install_walltime_s: int | None,
    what: str = "the pipeline",
) -> None:
    """Raise the refusal for one of the site script's typed exits; return on any other."""
    code = int(exit_code) if isinstance(exit_code, str) and exit_code.isdigit() else exit_code
    both = f"{output}\n{error}"
    if code == jarvis_submit.EXIT_NO_JARVIS:
        raise jarvis_submit.missing_jarvis(error, site=site)
    install_failures.raise_for(code, both, site=site)
    if code == jarvis_submit.EXIT_DECISION:
        exceeds = install_time.exceeds_issue(both)
        if exceeds is not None:
            raise install_time.refusal(exceeds, site=site)
        report = jarvis_report.parse_install_report(output)
        if report is None:
            raise Refusal(
                RefusalReason.SUBMIT_FAILED,
                "JARVIS needs to install the package's software and printed no install "
                f"report: {(error.strip() or output.strip())[-500:] or 'no output'}",
                needs=jarvis_submit.NEEDS_INSTALL,
            )
        raise jarvis_report.install_needed(
            report, site=site, walltime=jarvis_submit.install_walltime(install_walltime_s)
        )
    if code == jarvis_submit.EXIT_BUILD_REFUSED:
        raise build_refused(both, site=site)
    if code == jarvis_submit.EXIT_REFUSED:
        raise jarvis_submit.jarvis_refused(both, site=site, what=what)


def failure_text(output: str, error: str) -> str:
    """Say why a JARVIS command failed: its ``Error:`` lines, else stderr, else stdout.

    JARVIS prints a failed command's error on stdout, followed by the command's
    usage, while stderr may hold only notices such as "Auto-built environment".
    """
    lines = [line.strip() for line in output.splitlines() if line.lstrip().startswith("Error:")]
    return "\n".join(lines) or error.strip() or output.strip()


def checked(output: str) -> dict[str, Any]:
    """Return the install report a ``check`` that exited 0 printed."""
    report = jarvis_report.parse_install_report(output)
    if report is None:
        raise Refusal(
            RefusalReason.SUBMIT_FAILED,
            "jarvis ppl install +check printed no install report: "
            f"{output.strip()[-500:] or 'no output'}",
            scheduler="jarvis",
        )
    return report


CHECKED_HERE = "checked_here"
"""The install handle's data key when an ``install`` or ``recheck`` needed no job.

JARVIS's report on the submitting host said the software is all there: the
site chose to run JARVIS's check there (``install_placement``), or a record
already said so.
"""

LOGIN_BUILD = "login_build"
"""The install handle's data key for JARVIS's build on the submitting host."""

LOGIN_BUILD_SCHEMA = "clio-relay.login-build.v1"
"""The document the site script prints when it started JARVIS's build on its host."""


def checked_here(
    adapter: str, exit_code: int | str | None, output: str, handle: object
) -> JobHandle | None:
    """Return the handle of an install that JARVIS answered on the submitting host.

    It is None unless the install exited 0 with JARVIS's install report and printed
    no job ``handle``.
    """
    code = str(exit_code) if exit_code is not None else None
    if handle is not None or code != "0":
        return None
    if jarvis_report.parse_install_report(output) is None:
        return None
    return JobHandle(adapter=adapter, data={CHECKED_HERE: True, "install": True})


def login_build(
    adapter: str, exit_code: int | str | None, output: str, *, mode: str, **data: Any
) -> JobHandle | None:
    """Return the handle of JARVIS's build on the submitting host, if the install started one.

    The site placed the build there (``install_placement`` ``login_limited``), so
    the site script started JARVIS's ``ppl install --answers`` detached under the
    supervisor files the adapter already follows for ``mode`` (``detached`` over
    SSH, ``supervised`` on the host itself). ``data`` is the rest of the handle's.
    """
    code = str(exit_code) if exit_code is not None else None
    found = jarvis_submit.parse_document(output, LOGIN_BUILD_SCHEMA) if code == "0" else None
    if found is None:
        return None
    directory, pid = found.get("dir"), found.get("pid")
    if not isinstance(directory, str) or not directory:
        return None
    if not isinstance(pid, int) or isinstance(pid, bool):
        return None
    record: dict[str, Any] = {
        **data,
        "mode": mode,
        "job_dir": directory,
        "install": True,
        LOGIN_BUILD: {
            "pid": pid,
            "host": found.get("host"),
            "log": found.get("log"),
            "dir": directory,
        },
    }
    if mode == "supervised":
        record["supervisor_pid"] = pid
    return JobHandle(adapter=adapter, native_id=f"pid:{pid}", data=record)


def without_job(
    adapter: str,
    exit_code: int | str | None,
    output: str,
    handle: object,
    mode: str,
    data: dict[str, Any] | None = None,
) -> JobHandle | None:
    """Return the handle of an install that sent no job: :func:`login_build`'s or
    :func:`checked_here`'s. ``mode`` and ``data`` are as in :func:`login_build`."""
    built = login_build(adapter, exit_code, output, mode=mode, **(data or {}))
    return built or checked_here(adapter, exit_code, output, handle)


def _data(handle: JobHandle) -> dict[str, Any]:
    remote = handle.data.get("remote")
    data = cast(dict[str, Any], remote).get("data") if isinstance(remote, dict) else handle.data
    return cast(dict[str, Any], data) if isinstance(data, dict) else {}


def ran_here(handle: JobHandle) -> bool:
    """Say whether an install handle is :func:`checked_here`'s, also through a worker."""
    return _data(handle).get(CHECKED_HERE) is True


def built_here(handle: JobHandle) -> dict[str, Any] | None:
    """Return :func:`login_build`'s record of an install handle, also through a worker."""
    found = _data(handle).get(LOGIN_BUILD)
    return cast(dict[str, Any], found) if isinstance(found, dict) else None


def build_refused(text: str, *, site: str) -> Refusal:
    """The refusal for a JARVIS command that exited 4: it would not build where it was asked.

    ``text`` is the command's output; its install report's ``refused`` says what
    and which packages. JARVIS builds outside an allocation only where the site's
    ``install_placement`` answer put the build on the login host, and only while
    that host has room for JARVIS's limits, so this is final, not a retry: a later
    install asks JARVIS again.
    """
    refused: dict[str, Any] = {}
    document = _last_json(text)
    if isinstance(document.get("refused"), dict):
        refused = cast(dict[str, Any], document["refused"])
    what = refused.get("what") if isinstance(refused.get("what"), str) else "build"
    packages = [str(item) for item in cast(list[Any], refused.get("packages") or [])]
    named = f" ({', '.join(packages)})" if packages else ""
    return Refusal(
        RefusalReason.POLICY_REFUSED,
        f"JARVIS on {site} refused to {what}{named} outside an allocation: the site's "
        "build placement is an install job, or the login host had no room for JARVIS's "
        "limits. Relay does not retry this; installing again asks JARVIS where it builds",
        kind=refused.get("kind") or "build_outside_allocation",
        what=what,
        packages=packages,
        exit_code=jarvis_submit.EXIT_BUILD_REFUSED,
        retry=False,
    )


def _last_json(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    for start in reversed([match.start() for match in re.finditer(r"(?m)^\s*\{", text)]):
        try:
            found, _end = decoder.raw_decode(text[start:].lstrip())
        except ValueError:
            continue
        if isinstance(found, dict) and "refused" in found:
            return cast(dict[str, Any], found)
    return {}


def install_only_refused(scheduler: str | None, *, site: str) -> Refusal:
    """An install ahead of a run on a site where JARVIS cannot submit its install job."""
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"installing a package's software ahead of a run is a job JARVIS submits to Slurm "
        f"(ppl install +submit), and {site} has {scheduler or 'no batch scheduler'}; "
        "run the package instead",
        scheduler=scheduler,
        needs="a Slurm site",
    )

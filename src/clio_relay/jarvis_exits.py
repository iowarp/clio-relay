"""The site script's typed exits, as refusals, for every adapter that runs it.

The site script (:data:`~clio_relay.jarvis_submit.SITE_SCRIPT`) exits 69 when
relay's JARVIS is missing, 3 when JARVIS needs software installed or a choice,
4 when JARVIS refused to build outside an allocation, 5 when JARVIS refused the
pipeline and 6 for a typed install failure. Its ``check`` mode
(``ppl install +check``) exits 0 with JARVIS's install report when the
package's software is all there.
"""

from __future__ import annotations

from typing import Any

from clio_relay import install_failures, jarvis_submit
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec


def refuse(
    exit_code: int | str | None,
    output: str,
    error: str,
    *,
    spec: JobSpec,
    site: str,
    install_walltime_s: int,
    what: str = "the pipeline",
) -> None:
    """Raise the refusal for one of the site script's typed exits; return on any other."""
    code = int(exit_code) if isinstance(exit_code, str) and exit_code.isdigit() else exit_code
    both = f"{output}\n{error}"
    if code == jarvis_submit.EXIT_NO_JARVIS:
        raise jarvis_submit.missing_jarvis(error, site=site)
    install_failures.raise_for(code, both, site=site)
    if code == jarvis_submit.EXIT_DECISION:
        report = jarvis_submit.parse_install_report(output)
        if report is None:
            raise Refusal(
                RefusalReason.SUBMIT_FAILED,
                "JARVIS needs to install the package's software and printed no install "
                f"report: {(error.strip() or output.strip())[-500:] or 'no output'}",
                needs=jarvis_submit.NEEDS_INSTALL,
            )
        raise jarvis_submit.install_needed(
            report, site=site, walltime=jarvis_submit.install_walltime(spec, install_walltime_s)
        )
    if code == jarvis_submit.EXIT_BUILD_REFUSED:
        raise jarvis_submit.build_refused(both, site=site)
    if code == jarvis_submit.EXIT_REFUSED:
        raise jarvis_submit.jarvis_refused(both, site=site, what=what)


def checked(output: str) -> dict[str, Any]:
    """Return the install report a ``check`` that exited 0 printed."""
    report = jarvis_submit.parse_install_report(output)
    if report is None:
        raise Refusal(
            RefusalReason.SUBMIT_FAILED,
            "jarvis ppl install +check printed no install report: "
            f"{output.strip()[-500:] or 'no output'}",
            scheduler="jarvis",
        )
    return report


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

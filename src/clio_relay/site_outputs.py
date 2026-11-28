"""Where a site's runs write: the outputs directory, outside relay's own directories.

``site clean`` deletes relay's deployment on a site, so a run's outputs never
belong there. Each remote adapter has an ``outputs_dir`` setting (default
:data:`OUTPUTS_DIR` under the home directory), refused when it is inside
relay's own directory. The adapter hands it to relay's setup, which makes it
JARVIS's shared directory in the site's root, and to JARVIS's site script as
:data:`OUTPUTS_ENV` in the job's environment (:func:`with_outputs`), which
checks it is still that one. So each pipeline writes ``<outputs>/<operation
id>``, which outlives the job directory and relay's own.
"""

from __future__ import annotations

import posixpath

from clio_relay.jobs.spec import JobSpec

OUTPUTS_ENV = "CLIO_RELAY_OUTPUTS"
"""The site script's setting naming the outputs directory; the script unsets it."""
OUTPUTS_DIR = "clio-relay-runs"
"""The default outputs directory of a remote site, relative to the home directory."""
HELP = (
    "Where runs write their outputs (JARVIS's shared directory, and a case's run_dir), "
    "relative to the home directory or absolute. It must be outside relay's own directory, "
    "which site clean deletes; site clean keeps it unless the person asks otherwise"
)


def _home_relative(path: str) -> str:
    """Return ``path`` with ``~/`` dropped and no trailing slash: relative paths are from home."""
    found = path.strip()
    found = "" if found == "~" else found.removeprefix("~/")
    return posixpath.normpath(found) if found else ""


def check_outputs_dir(outputs: str, *own: str | None) -> str:
    """Return the outputs directory, refusing one that is one of relay's ``own`` or inside it.

    A relative path and a ``~/`` path are both from the home directory, so they
    compare; an absolute one compares with an absolute directory only.
    """
    found = _home_relative(outputs)
    if found in ("", "."):
        raise ValueError("outputs_dir must be a directory of its own, not the home directory")
    for home in own:
        mine = _home_relative(home or "")
        if not mine or found.startswith("/") != mine.startswith("/"):
            continue
        if found == mine or found.startswith(mine.rstrip("/") + "/") or mine == ".":
            raise ValueError(
                f"outputs_dir {outputs} is inside relay's own directory {home}, which site "
                "clean deletes; pick a directory outside it"
            )
    return outputs


def dir_facts(own: str | None, outputs: str | None) -> dict[str, str | None]:
    """Return describe's facts on relay's ``own`` directory and the site's outputs directory."""
    return {"remote_dir": own, "outputs_dir": outputs}


def with_outputs(spec: JobSpec, outputs: str | None) -> JobSpec:
    """Return ``spec`` with the site's outputs directory for JARVIS's site script, if set."""
    if not outputs:
        return spec
    return spec.model_copy(update={"environment": {**spec.environment, OUTPUTS_ENV: outputs}})

"""A package job handed to JARVIS: the pipeline, the site script, the records (RL40, RL45).

JARVIS owns a package job from end to end. Relay renders one pipeline YAML
from the job, with the run's resources in its ``scheduler:`` block, and a site
script hands it to JARVIS:

- ``submit`` runs ``jarvis ppl submit <pipeline> --execution-id <op> +json`` on
  the login host. JARVIS seals the execution directory, writes the typed
  ``.jarvis-execution.json`` record and the allocation hostfile, binds the
  interpreter and ``JARVIS_ROOT``, writes ``submit.slurm`` and calls ``sbatch``.
  The ssh adapter runs it with its own ``ssh``; the Tapis adapter runs it in a
  FORK job.
- ``run`` runs ``jarvis ppl run yaml <pipeline> +json``, JARVIS's direct run,
  for an adapter that starts the job itself (no batch scheduler).
- ``install`` runs ``jarvis ppl install yaml <pipeline> --answers <file>
  +submit --execution-id <op>-install +json``: JARVIS installs what the
  pipeline's packages need, as a job of its own; a later round is <op>-install-2 on.
  ``recheck`` adds ``+check``: JARVIS only checks in that job.

The login host runs no Spack: for a pipeline with a ``scheduler:`` block,
JARVIS's ``ppl install +check`` there reads the ``install-check.json`` record
a job's check wrote. A submit asks that light check first. When software is
missing, needs a decision, or has no matching record (``install_unchecked``),
the script writes JARVIS's ``jarvis.install-report.v1`` to
``relay.install-report.json`` and exits 3, before any ``sbatch``. Relay asks
the person to approve the install and answer each question
(:func:`install_needed`), or asks nothing and runs ``recheck`` for
``install_unchecked``, follows that job, and then submits the run again.

Either way the job's JARVIS root is ``<job dir>/.relay-jarvis``, private to the
job, so the person's own pipelines are never read or changed. Status, outputs,
services, cancel and stop all go through JARVIS's ``execution`` commands and its
record, never through relay's own tracking of the batch job or the scheduler.

Relay always runs its own pinned JARVIS, by absolute path, never a ``jarvis``
found on the site's ``PATH``: an older one would reject the service reports and
skip JARVIS's refusal to build on the login node. :data:`BOOTSTRAP_SCRIPT` puts
jarvis-cd at :data:`JARVIS_PIN` under the CLIO site home, after the person
approves it, and the site script refuses when it is missing or at another pin.
A site that needs another JARVIS names its path in the adapter's ``jarvis``
setting (:func:`explicit_jarvis`).
"""

from __future__ import annotations

import hashlib
import json
import math
import posixpath
import re
import shlex
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Literal, cast

import yaml

from clio_relay.jarvis_bootstrap import BOOTSTRAP_SCRIPT as BOOTSTRAP_SCRIPT
from clio_relay.jarvis_bootstrap import PYTHON_SETTING_HELP as PYTHON_SETTING_HELP
from clio_relay.jarvis_bootstrap import explicit_python as explicit_python
from clio_relay.jarvis_graph import NO_BUILTIN_ENV
from clio_relay.jarvis_graph import PKG_TYPE as GRAPH_PKG_TYPE
from clio_relay.jarvis_record import install_progress, scheduler_start
from clio_relay.jarvis_site_script import SITE_SCRIPT as SITE_SCRIPT
from clio_relay.jobs.handles import VIEW_METADATA, JobState, JobStatus, OutputRef
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import CustomValue, InputRef, JobSpec, PackageRef, step_id, step_ids

ARCHIVE_DIR = ".relay-package"
"""Where the site script finds the package archive, relative to the job directory."""

JARVIS_DIR = ".relay-jarvis"
"""The job's private JARVIS root, relative to the job directory."""

PIPELINE_FILE = "relay.pipeline.yaml"
SCRIPT_FILE = "relay-jarvis.sh"
SUBMISSION_FILE = "relay.submission.json"
"""The ``jarvis.execution.handle.v1`` line ``ppl submit`` printed."""
REFUSAL_FILE = "relay.refusal.json"
"""Written when the site has no JARVIS, or no Spack for it: ``{"reason", "missing", "tried"}``."""
EXITCODE_FILE = "relay.exitcode"
JARVIS_FILE = "relay.jarvis"
"""The JARVIS program the site script found, for the ``execution`` commands later."""
INSTALL_REPORT_FILE = "relay.install-report.json"
"""The ``jarvis.install-report.v1`` of a submit that exited 3."""
ANSWERS_FILE = "relay.answers.json"
"""The person's answers to JARVIS's install questions, ``{issue_id: {constraint, ...}}``."""
INSTALL_FILE = "relay.install.json"
"""The ``jarvis.execution.handle.v1`` line ``ppl install +submit`` printed."""
SHARED_FILE = "relay.shared"
"""Where the job's JARVIS shared directory really is: the outputs directory's, or the root's."""
SHARED_SCHEMA = "clio-relay.shared.v1"
"""The line the site script prints with ``path``, the same as :data:`SHARED_FILE`."""

SITE_FILES = (
    PIPELINE_FILE,
    SCRIPT_FILE,
    SUBMISSION_FILE,
    REFUSAL_FILE,
    EXITCODE_FILE,
    JARVIS_FILE,
    INSTALL_REPORT_FILE,
    ANSWERS_FILE,
    INSTALL_FILE,
    SHARED_FILE,
)
"""Files the site script keeps in the job directory. They are not outputs."""

HANDLE_SCHEMA = "jarvis.execution.handle.v1"
RECORD_SCHEMA = "jarvis.execution.record.v1"
ARTIFACTS_SCHEMA = "jarvis.execution.artifacts.v1"
SERVICE_RUNTIMES_SCHEMA = "jarvis.execution.service-runtimes.v1"
INSTALL_REPORT_SCHEMA = "jarvis.install-report.v1"
RECORD_FILE = ".jarvis-execution.json"

JARVIS_VERSION = "1.9.5"
"""The jarvis-cd release relay installs under the CLIO site home and runs."""
JARVIS_PIN = "e2b52030357dbdadf1d898c8242f33c2864638ba"
"""The commit the jarvis-cd :data:`JARVIS_VERSION` tag names. It is written to the pin file."""
JARVIS_WHEEL_SHA256 = "22ef9e9f108d87466c94be5d94eed8d97344011730f3a3d43bedd731cbe83842"
JARVIS_SOURCE = (
    f"https://github.com/grc-iit/jarvis-cd/releases/download/v{JARVIS_VERSION}/"
    f"jarvis_cd-{JARVIS_VERSION}-py3-none-any.whl#sha256={JARVIS_WHEEL_SHA256}"
)
"""The release wheel. pip checks the digest in the fragment before it installs anything."""
BOOTSTRAP_DIR = "jarvis-cd"
"""Where relay installs jarvis-cd, relative to the CLIO site home."""
FALLBACK_ENV = "CLIO_RELAY_JARVIS_FALLBACK"
"""Relay's pinned JARVIS (:func:`bootstrap_path`), which the site script runs unless the
site's setting names another by path."""
PIN_ENV = "CLIO_RELAY_JARVIS_PIN"
"""The pin the site script expects in ``<home>/jarvis-cd/pin`` beside relay's JARVIS."""
JARVIS_SETTING_HELP = (
    "Leave empty: relay runs its own pinned jarvis-cd, installed under the CLIO site home. "
    "Only a site that needs another jarvis names its path here, absolute or from ~/. Relay "
    "never runs a jarvis found on the site's PATH"
)
"""The description of an adapter's ``jarvis`` setting (:func:`explicit_jarvis`)."""
GRAPH_SETTING_HELP = (
    "Leave empty: JARVIS keeps the site's resource graph under the CLIO site home. A site "
    "that publishes its graph names that file here, absolute or from ~/. JARVIS reads and "
    "writes it, including the install answers it shares with other homes, and site clean "
    "keeps it"
)
"""The description of an adapter's ``jarvis_graph`` setting (:func:`published_graph`)."""
PIPELINES_DIR = "pipelines"
"""Where the Spack env of each set of archives is kept, under :data:`BOOTSTRAP_DIR`."""
SITE_DIR = "site"
"""JARVIS's site record under :data:`BOOTSTRAP_DIR`: the resource graph and the decisions."""
GRAPH_FILE = "resource_graph.yaml"
"""The site's JARVIS resource graph, in :data:`SITE_DIR`."""
DECISIONS_FILE = "decisions.yaml"
"""The site's JARVIS install decisions, in :data:`SITE_DIR`, beside the graph."""
GRAPH_ENV = "CLIO_RELAY_JARVIS_GRAPH"
"""The file the site script exports as ``JARVIS_RESOURCE_GRAPH``."""
DECISIONS_ENV = "CLIO_RELAY_JARVIS_DECISIONS"
"""The file the site script exports as ``JARVIS_SPACK_DECISIONS``."""
SPACK_ENV_ENV = "CLIO_RELAY_JARVIS_SPACK_ENV"
"""The directory the site script exports as ``JARVIS_SPACK_ENV_DIR``."""
INSTALL_ENV_ENV = "CLIO_RELAY_JARVIS_INSTALL_ENV"
"""The directory the site script exports as ``JARVIS_INSTALL_ENV_DIR``: the pip venvs and
conda envs, shared by the install job and the runs as the Spack env is."""
SPACK_DIR = "spack"
"""The site's JARVIS Spack, relative to the CLIO site home (``jarvis spack prepare --root``)."""
SPACK_CONFIG_DIR = "spack-config"
"""An older CLIO Spack configuration directory, which ``site clean`` still removes."""
SPACK_ROOT_ENV = "CLIO_RELAY_SPACK_ROOT"
"""The Spack the site script exports as ``JARVIS_SPACK_ROOT`` when it is there."""

EXIT_DECISION = 3
"""JARVIS needs to install software, or a decision, before the job can run."""
EXIT_BUILD_REFUSED = 4
"""JARVIS refused to build outside an allocation (``refused.kind: build_outside_allocation``):
the site's build placement is a job, or the login host had no room for JARVIS's limits.
Relay never retries it."""
EXIT_USAGE = 64
"""A bad mode, package name or execution id."""
EXIT_DIGEST = 65
"""The archive's SHA-256 is not the reviewed one."""
EXIT_MISSING = 66
"""The archive, the pipeline or the archive's repository directory is missing."""
EXIT_NO_JARVIS = 69
"""The site has no JARVIS where the site's setting says."""

EXIT_REFUSED = 5
"""JARVIS refused with a ``jarvis.refusal.v1`` document of typed issues (``ppl check``,
``execution move``)."""
RECHECK = "clio_relay_recheck"
"""The key in :func:`install_command`'s answers that makes it a ``recheck``: a job in
which JARVIS checks the software with the person's answers, and builds nothing."""
INSTALL_SCHEDULER = "clio_relay_install_scheduler"
"""The key in :func:`install_command`'s answers for the ``scheduler.install`` settings
the person picked (``{"partition": ...}`` or ``{"time": ...}``) when JARVIS's build
estimate was longer than the partition allows (:mod:`clio_relay.install_time`)."""
RELAY_ANSWERS = (RECHECK, INSTALL_SCHEDULER)
"""Relay's own keys in the answers; JARVIS's answers file gets none of them."""

REFUSAL_SCHEMA = "jarvis.refusal.v1"
CHECK_SCHEMA = "jarvis.pipeline-check.v1"
CHECK_FILE = "relay.check.json"
"""``jarvis ppl check +json`` of the job's pipeline, which the site script keeps."""
SERVICE_NODE_FILE = "relay.service-node"
"""The allocation index of the service's first host, from :data:`CHECK_FILE`."""

ExecutionVerb = Literal[
    "get", "progress", "artifacts", "service-runtimes", "cancel", "stop", "move"
]
EXECUTION_VERBS: tuple[ExecutionVerb, ...] = (
    "get",
    "progress",
    "artifacts",
    "service-runtimes",
    "cancel",
    "stop",
    "move",
)

NEEDS_INSTALL = "jarvis ppl install"

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_TOKEN = re.compile(r"[^\s#]+")
_NAME = re.compile(r"[a-z][a-z0-9_]*")
_RESERVED_PARAMETERS = frozenset({"pkg_type", "pkg_name"})
_SLURM_PREFIX = "slurm."

# The ``scheduler:`` keys relay sets itself. A ``slurm.<key>`` custom attribute
# may not replace them.
_OWN_KEYS = frozenset(
    {
        "name",
        "job_name",
        "nodes",
        "ntasks",
        "ntasks_per_node",
        "cpus_per_task",
        "mem",
        "time",
        "partition",
        "account",
        "reservation",
        "exclusive",
        "gpus_per_node",
        "output",
        "error",
        "hostfile",
        "suffix",
        "sbatch_args",
        "pre_cmds",
        "post_cmds",
    }
)


# Names and paths


def pipeline_id(operation_id: str) -> str:
    """Return the JARVIS pipeline id of an operation's package job."""
    return f"relay_{operation_id}"


def install_execution_id(operation_id: str) -> str:
    """Return the execution id of the job that installs an operation's software.

    It lives in the operation's pipeline, ``pipeline_id(operation_id)``.
    """
    return f"{operation_id}-install"


def bootstrap_path(home: str) -> str:
    """Return where :data:`BOOTSTRAP_SCRIPT` links the JARVIS it installs under ``home``."""
    return posixpath.join(home, BOOTSTRAP_DIR, "jarvis")


def explicit_jarvis(value: str) -> str:
    """Check an adapter's ``jarvis`` setting; empty means relay's pinned JARVIS.

    A site that needs another JARVIS names it by path: absolute, or from the
    home directory with a leading ``~/``. A bare name would be looked up on the
    site's ``PATH``, which relay never does, so it is refused. ``jarvis``, the
    old default, means relay's pinned JARVIS.
    """
    value = value.strip()
    if value in ("", "jarvis"):
        return ""
    if not value.startswith(("/", "~/")):
        raise ValueError(
            f"jarvis must be a path (absolute or ~/...), not {value!r}: relay never runs a "
            "jarvis found on the site's PATH. Leave it empty to use relay's pinned jarvis-cd"
        )
    return value


def published_graph(value: str) -> str:
    """Check an adapter's ``jarvis_graph`` setting; empty means the home's own graph.

    The site's published resource graph is named by path on the site: absolute,
    or from the home directory with a leading ``~/``.
    """
    value = value.strip()
    if value and not value.startswith(("/", "~/")):
        raise ValueError(
            f"jarvis_graph must be a path on the site (absolute or ~/...), not {value!r}. "
            "Leave it empty to keep the graph under the CLIO site home"
        )
    return value


def site_graph(home: str, published: str = "") -> str:
    """Return the site's JARVIS resource graph: the published one, else the home's own.

    ``published`` is the site's ``jarvis_graph`` setting (:func:`published_graph`).
    Without it the graph is kept under the CLIO site home ``home``.
    """
    return published or posixpath.join(home, BOOTSTRAP_DIR, SITE_DIR, GRAPH_FILE)


def site_decisions(home: str) -> str:
    """Return the site's JARVIS install decisions file, beside :func:`site_graph`."""
    return posixpath.join(home, BOOTSTRAP_DIR, SITE_DIR, DECISIONS_FILE)


def site_root(home: str) -> str:
    """Return the JARVIS root relay uses for site-level reads, such as ``rg net show``."""
    return posixpath.join(home, BOOTSTRAP_DIR, SITE_DIR, "root")


GRAPH_SCHEMA = "jarvis.resource-graph.net-report.v1"
"""What ``jarvis rg net show +json`` prints: the site graph's networks and the chosen ones."""


def graph_pipeline(scheduler: str, *, nodes: int = 2, time: str = "00:05:00") -> dict[str, Any]:
    """Return the pipeline of relay's network collection on compute nodes.

    It is relay's own package (:mod:`clio_relay.jarvis_graph`, ``net_only``),
    submitted as a job of ``nodes`` nodes on the site's scheduler, which JARVIS
    detects, so the networks are the compute nodes' and never a login host's.
    """
    document: dict[str, Any] = {
        "name": "site_net",
        "pkgs": [{"pkg_type": GRAPH_PKG_TYPE}],
    }
    if scheduler not in ("", "none"):
        document["scheduler"] = {"nodes": nodes, "time": time}
    return document


def graph_report(text: str) -> dict[str, Any] | None:
    """Return JARVIS's net report from ``rg net show +json`` output, or ``None``."""
    start = text.find("{")
    if start < 0:
        return None
    try:
        found, _end = json.JSONDecoder().raw_decode(text[start:])
    except ValueError:
        return None
    return cast(dict[str, Any], found) if isinstance(found, dict) else None


def site_environment(spec: JobSpec, home: str | None, graph: str = "") -> dict[str, str]:
    """Return the site script's settings for ``spec`` and the CLIO site home ``home``.

    Relay's pinned JARVIS there (:func:`bootstrap_path`) is the one the script
    runs, and :data:`JARVIS_PIN` is the pin it must carry. A relative ``home``
    is from the home directory, as ssh's ``remote_dir`` is.

    The job's JARVIS root is per operation, so the site's facts are kept where
    every root finds them: the resource graph (:func:`site_graph`, exported as
    ``JARVIS_RESOURCE_GRAPH``) and the install decisions JARVIS records and
    replays (:func:`site_decisions`, exported as ``JARVIS_SPACK_DECISIONS``).
    Relay keeps no record of its own of either. The pipeline's Spack
    environment, and its pip venvs and conda envs (``install-envs``, exported
    as ``JARVIS_INSTALL_ENV_DIR``), are kept at
    ``<home>/jarvis-cd/pipelines/<steps digest>/``,
    keyed by the set of archives (:func:`steps_digest`), so a repeat run does
    not concretize again.

    The site's JARVIS Spack, ``<home>/spack``, is offered too: the root
    ``jarvis spack prepare`` chains to the site's own Spack, with its
    configuration in the root's site scope. The site script exports it as
    ``JARVIS_SPACK_ROOT`` only when the checkout has ``bin/spack``, so a site
    without one keeps JARVIS's own lookup, and JARVIS asks ``spack_missing``
    when it finds none.

    ``graph`` is the site's published resource graph (``jarvis_graph``), which
    replaces the home's own when the site names one.
    """
    if not home:
        return {}
    kept = posixpath.join(home, BOOTSTRAP_DIR, PIPELINES_DIR, steps_digest(spec))
    return {
        FALLBACK_ENV: bootstrap_path(home),
        PIN_ENV: JARVIS_PIN,
        GRAPH_ENV: site_graph(home, graph),
        DECISIONS_ENV: site_decisions(home),
        SPACK_ENV_ENV: posixpath.join(kept, "spack-env"),
        INSTALL_ENV_ENV: posixpath.join(kept, "install-envs"),
        SPACK_ROOT_ENV: posixpath.join(home, SPACK_DIR),
    }


def jarvis_root(job_dir: str) -> str:
    """Return the job's private JARVIS root."""
    return posixpath.join(job_dir, JARVIS_DIR)


def execution_dir(
    job_dir: str, execution_id: str, *, pipeline: str | None = None, shared: str | None = None
) -> str:
    """Return the execution directory JARVIS seals for an execution.

    ``pipeline`` defaults to the operation's, ``pipeline_id(execution_id)``.
    ``shared`` is where the site script says the shared directory is
    (:func:`shared_dir`); without it, the job root's ``shared``.
    """
    return posixpath.join(
        shared or posixpath.join(jarvis_root(job_dir), "shared"),
        pipeline or pipeline_id(execution_id),
        "executions",
        execution_id,
    )


def shared_dir(text: str) -> str | None:
    """Return the shared directory a site script's output names, if it names one."""
    found = parse_document(text, SHARED_SCHEMA)
    path = found.get("path") if found else None
    if not isinstance(path, str) or not path.startswith("/"):
        return None
    return path.rstrip("/") or "/"


def record_path(job_dir: str, execution_id: str, *, pipeline: str | None = None) -> str:
    """Return the path of the execution's ``jarvis.execution.record.v1`` file."""
    return posixpath.join(execution_dir(job_dir, execution_id, pipeline=pipeline), RECORD_FILE)


def steps_of(spec: JobSpec) -> list[PackageRef]:
    """Return the pipeline's packages, refusing a job that has none."""
    steps = spec.steps()
    if not steps:
        raise ValueError("not a package job")
    return steps


def archive_inputs(spec: JobSpec) -> list[InputRef]:
    """Return the inputs that stage each reviewed package archive by its digest, once each."""
    found: dict[str, InputRef] = {}
    for step in steps_of(spec):
        digest = step.pinned()[1]
        found.setdefault(digest, InputRef(name=f"{ARCHIVE_DIR}/{digest}.tar.gz", sha256=digest))
    return list(found.values())


def with_archive(spec: JobSpec) -> JobSpec:
    """Return ``spec`` with every package archive among its inputs, once."""
    archives = archive_inputs(spec)
    names = {item.name for item in archives}
    inputs = [item for item in spec.inputs if item.name not in names]
    return spec.model_copy(update={"inputs": [*inputs, *archives]})


def steps_digest(spec: JobSpec) -> str:
    """Return the sha256 of the sorted package archive digests: the pipeline's software."""
    digests = sorted({step.pinned()[1] for step in steps_of(spec)})
    return hashlib.sha256(",".join(digests).encode()).hexdigest()


def package_names(steps: list[PackageRef]) -> dict[str, str]:
    """Map each step's ``pkg_name`` (:func:`step_names`) to its package's name."""
    return dict(zip(step_names(steps), (step.name for step in steps), strict=True))


def step_names(steps: list[PackageRef]) -> list[str]:
    """Return each step's ``pkg_name`` in the pipeline (:func:`~clio_relay.jobs.spec.step_ids`)."""
    return step_ids(steps)


def pipeline_couplings(spec: JobSpec) -> list[dict[str, str]]:
    """Return the job's couplings as JARVIS's ``couplings:``, each end a step's ``pkg_name``."""
    steps = spec.steps()
    found: list[dict[str, str]] = []
    for edge in spec.couplings:
        item = {
            "from": step_id(steps, edge.producer),
            "to": step_id(steps, edge.consumer),
            "kind": edge.kind,
        }
        if edge.label:
            item["label"] = edge.label
        if item not in found:
            found.append(item)
    return found


# The pipeline


def slurm_time(duration: timedelta) -> str:
    """Render a walltime as Slurm's ``[D-]HH:MM:SS``, rounding seconds up."""
    seconds = max(1, math.ceil(duration.total_seconds()))
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    clock = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{days}-{clock}" if days else clock


def _token(key: str, value: object) -> object:
    if isinstance(value, str) and not _TOKEN.fullmatch(value):
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            f"JARVIS's scheduler block takes {key} as one word without spaces or '#', "
            f"not {value!r}",
            field=key,
        )
    return value


def scheduler_block(
    spec: JobSpec,
    *,
    job_name: str,
    stdout: str | None = None,
    stderr: str | None = None,
    job_dir: str | None = None,
) -> dict[str, object]:
    """Return the pipeline's ``scheduler:`` block for ``spec``, naming no scheduler.

    JARVIS detects it (typed ``scheduler_missing``). The resources and walltime
    are those the spend ask priced: ``nodes()``, ``ranks_per_node()`` and ``duration``.
    A ``slurm.<key>`` custom attribute adds ``--<key>``. With no duration, JARVIS
    writes no ``--time`` and the scheduler's default applies. With no ``stdout``
    or ``stderr``, JARVIS writes the logs in its execution directory and
    registers them as the ``stdout`` and ``stderr`` artifacts.

    For a package service (:func:`is_service`) ``job_dir`` is required: the
    block's ``pre_cmds`` start relay's connector, ``serve/connect.sh``, beside
    the batch shell (:func:`connector_command`).
    """
    resources = spec.resources
    attributes = spec.attributes
    block: dict[str, object] = {"job_name": job_name, "nodes": resources.nodes()}
    ranks = resources.ranks_per_node()
    if ranks is not None:
        block["ntasks_per_node"] = ranks
    cores = resources.cpu_cores_per_process
    if cores is not None and cores > 1:
        block["cpus_per_task"] = cores
    if resources.memory is not None:
        block["mem"] = f"{math.ceil(resources.memory / 2**20)}M"
    if resources.gpu_cores_per_process is not None:
        block["gpus_per_node"] = resources.gpu_cores_per_process * (ranks or 1)
    if resources.exclusive_node_use:
        block["exclusive"] = True
    if attributes.duration is not None:
        block["time"] = slurm_time(attributes.duration)
    if attributes.queue_name:
        block["partition"] = attributes.queue_name
    account = attributes.account or attributes.project_name
    if account:
        block["account"] = account
    if attributes.reservation_id:
        block["reservation"] = attributes.reservation_id
    for name, value in sorted(attributes.custom_attributes.items()):
        if not name.startswith(_SLURM_PREFIX):
            continue
        key = name[len(_SLURM_PREFIX) :].replace("-", "_")
        if key in _OWN_KEYS or not _NAME.fullmatch(key):
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"the custom attribute {name} cannot set the scheduler's {key}",
                field=name,
            )
        block[key] = value
    if stdout is not None:
        block["output"] = stdout
    if stderr is not None:
        block["error"] = stderr
    checked: dict[str, object] = {key: _token(key, value) for key, value in block.items()}
    if is_service(spec):
        if job_dir is None:
            raise ValueError("a package service's scheduler block needs the job directory")
        checked["pre_cmds"] = [connector_command(spec, job_dir=job_dir)]
    return checked


CONNECTOR = "serve/connect.sh"
"""Relay's connector for a package service, relative to the job directory."""


def is_service(spec: JobSpec) -> bool:
    """Return whether ``spec`` is a package job run as a relay service."""
    return any(step.service for step in spec.steps())


_SLICE_HOST = (
    'h=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | awk -v s="$s" '
    "'{ h[NR - 1] = $0 } END { if (s < 0) s += NR; if (s >= 0 && s < NR) print h[s] }'); "
    'if [ -z "$h" ]; then echo "no host $s in the allocation $SLURM_JOB_NODELIST"; '
    "exit 1; fi; "
)


def connector_command(spec: JobSpec, *, job_dir: str) -> str:
    """Return the ``pre_cmds`` line that starts the connector on the service's first host.

    The connector reads ``serve/ports`` and ``serve/secrets.env``, runs frpc
    with restarts, writes ``serve/ready.json`` once the ports listen on its
    host, writes ``serve/idle`` after the idle release and ends with the job.
    Its settings are arguments, so they do not depend on the environment JARVIS
    gives the batch shell.

    JARVIS says which step serves and where: the site script keeps the first
    allocation index of the first service ``jarvis ppl check`` names in
    :data:`SERVICE_NODE_FILE`. Relay does not work it out from the steps. When
    that index is 0 (or JARVIS names none) the connector runs beside the batch
    shell. Otherwise it is a one-task step of the job, ``srun --overlap`` on
    that host, which Slurm ends with the job. Either way it runs on a compute
    node, inside the allocation JARVIS submitted.
    """
    env = spec.environment
    job = shlex.quote(job_dir)
    arguments = shlex.join(
        [
            env.get("CLIO_RELAY_OPERATION", ""),
            env.get("CLIO_RELAY_IDLE_S", "0"),
            env.get("CLIO_RELAY_FRPC", "") or "-",
        ]
    )
    log = shlex.quote(posixpath.join(job_dir, "serve", "connect.log"))
    node = shlex.quote(posixpath.join(job_dir, SERVICE_NODE_FILE))
    inner = f"cd {job} && exec sh {CONNECTOR} {arguments}"
    step = f'exec srun --overlap --nodes=1 --ntasks=1 --nodelist="$h" sh -c {shlex.quote(inner)}'
    return (
        f's=$(cat {node} 2>/dev/null) || s=; case $s in ""|*[!0-9-]*) s=0 ;; esac; '
        f'if [ "$s" = 0 ]; then ({inner}) </dev/null >>{log} 2>&1 & '
        f"else ({_SLICE_HOST}{step}) </dev/null >>{log} 2>&1 & fi"
    )


def _parameter(key: str, value: CustomValue) -> CustomValue:
    if key in _RESERVED_PARAMETERS:
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            f"{key} names the package in a JARVIS pipeline, so it is not a parameter",
            field=key,
        )
    return value


def input_paths(spec: JobSpec, job_dir: str) -> dict[str, str]:
    """Return each run input's absolute path on the site, by the input's name.

    The archive is left out: it is relay's, not the package's.
    """
    return {
        item.name: posixpath.join(job_dir, item.name)
        for item in spec.inputs
        if not item.name.startswith(f"{ARCHIVE_DIR}/")
    }


_PLACEHOLDER = re.compile(r"[$%]|\{[^}]*\}|<[^>]*>")


def _resolved(key: str, value: CustomValue, paths: Mapping[str, str], package: str) -> CustomValue:
    """Return ``value`` with a run input's name turned into its absolute path.

    ``./name`` resolves as ``name`` does. A value that wraps an input's name in
    a placeholder such as ``{job_dir}/name`` or ``$JOB/name`` is refused: relay
    puts the path in itself, and nothing on the site expands the placeholder.
    """
    if not isinstance(value, str) or not paths:
        return value
    if value in paths:
        return paths[value]
    normal = posixpath.normpath(value) if not value.startswith("/") else value
    if normal in paths:
        return paths[normal]
    name = posixpath.basename(value)
    if name in paths and _PLACEHOLDER.search(value):
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            f"{key} is {value!r}, a placeholder path nothing on the site expands; "
            f"set {key} to the input's name {name!r} and relay puts its absolute path in",
            package=package,
        )
    return value


def pipeline_document(
    spec: JobSpec,
    *,
    pipeline: str,
    scheduler: Mapping[str, object] | None = None,
    job_dir: str | None = None,
) -> dict[str, object]:
    """Return the JARVIS pipeline for ``spec``'s packages, with typed parameters.

    Each package is one ``pkgs:`` entry, in order (:func:`step_names`), and all
    share the one ``scheduler:`` block. The order is the order JARVIS starts
    them in; which way data moves between them is the pipeline's ``couplings:``
    (:func:`pipeline_couplings`). ``host_slice``, ``start_mode`` and the
    package's other parameters pass through as they are. The pipeline names no
    ``env``, so JARVIS captures the environment it is loaded in, which is where
    the site script exports the job's environment.

    With ``job_dir``, a parameter whose value is a run input's name becomes that
    input's absolute path in the job directory, so the person or agent never
    spells out where relay puts the job. JARVIS runs the package in its own
    execution directory, where a relative name would not resolve.
    """
    steps = steps_of(spec)
    if not _ID.fullmatch(pipeline):
        raise Refusal(RefusalReason.INVALID_SPEC, f"{pipeline!r} is not a JARVIS pipeline id")
    if spec.directory is not None or spec.stdin_path is not None:
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            "JARVIS runs a package in its own execution directory with no standard input; "
            "leave directory and stdin_path out and set the package's own parameters",
            package=steps[0].name,
        )
    paths = input_paths(spec, job_dir) if job_dir is not None else {}
    entries: list[dict[str, object]] = []
    for step, name in zip(steps, step_names(steps), strict=True):
        entry: dict[str, object] = {"pkg_type": step.name, "pkg_name": name}
        entry.update(
            {
                key: _resolved(key, _parameter(key, value), paths, step.name)
                for key, value in sorted(step.parameters.items())
            }
        )
        entries.append(entry)
    document: dict[str, object] = {"name": pipeline}
    if scheduler is not None:
        document["scheduler"] = dict(scheduler)
    document["pkgs"] = entries
    couplings = pipeline_couplings(spec)
    if couplings:
        document["couplings"] = couplings
    return document


def render_pipeline(document: Mapping[str, object]) -> str:
    """Render a pipeline document as the YAML file JARVIS loads."""
    return yaml.safe_dump(dict(document), sort_keys=False, default_flow_style=False)


# The site script (clio_relay.jarvis_site_script)

SiteMode = Literal["submit", "plan", "run", "install", "check", "recheck"]


def steps_argument(spec: JobSpec) -> str:
    """Return the site script's ``<repo>.<package>@<sha256>,...`` for the pipeline."""
    return ",".join(f"{step.name}@{step.pinned()[1]}" for step in steps_of(spec))


def site_arguments(
    spec: JobSpec,
    *,
    mode: SiteMode,
    jarvis: str,
    execution: str = "",
    answers: Mapping[str, Any] | None = None,
    pipeline: str | None = None,
) -> list[str]:
    """Return the site script's arguments after ``$0``.

    ``answers`` is for ``install`` and ``recheck`` only. ``pipeline`` is the YAML
    text, for an adapter that cannot place ``relay.pipeline.yaml`` in the job
    directory itself.
    """
    arguments = [mode, jarvis, execution, steps_argument(spec)]
    if mode in ("install", "recheck"):
        arguments.append(json.dumps(dict(answers or {}), sort_keys=True, separators=(",", ":")))
    return arguments if pipeline is None else [*arguments, pipeline]


def _site_command(
    spec: JobSpec, arguments: list[str], *, script: str, home: str | None, graph: str = ""
) -> str:
    settings = {**spec.environment, **site_environment(spec, home, graph)}
    environment = [f"{key}={value}" for key, value in settings.items()]
    prefix = ["env", *environment] if environment else []
    return shlex.join([*prefix, "bash", script, *arguments])


def submit_command(
    spec: JobSpec,
    *,
    operation_id: str,
    jarvis: str,
    script: str = SCRIPT_FILE,
    home: str | None = None,
    mode: SiteMode = "submit",
    graph: str = "",
) -> str:
    """Return the shell command that submits ``spec`` from its job directory.

    The job's environment is exported first, so JARVIS captures it in the
    pipeline it seals. ``home`` is the CLIO site home, for
    :func:`site_environment`: the JARVIS relay installed there is used when
    ``jarvis`` points to nothing, and the person's install answers are kept there.
    ``graph`` is the site's published resource graph, when it names one.
    ``mode="check"`` checks the software instead, for an install ahead of a run.
    """
    arguments = site_arguments(spec, mode=mode, jarvis=jarvis, execution=operation_id)
    return _site_command(spec, arguments, script=script, home=home, graph=graph)


INSTALL_WALLTIME_HELP = (
    "The walltime, in seconds, of the one-node job JARVIS builds a package's software in. "
    "Unset (the default), JARVIS sizes it from its own estimate of the build, within the "
    "partition's limit. Set it only to override JARVIS, within the site's partition limit"
)

INSTALL_PIPELINE_FILE = "relay.install-pipeline.yaml"
"""The pipeline the install job builds with, beside the run's :data:`PIPELINE_FILE`."""

INSTALL_CHECK_WALLTIME_S = 3600
"""The longest walltime of a ``recheck`` job when the site sets an install walltime."""


def is_recheck(answers: Mapping[str, Any]) -> bool:
    """Say whether :func:`install_command`'s ``answers`` make it a ``recheck``."""
    return bool(answers.get(RECHECK))


def install_walltime(walltime_s: int | None) -> str | None:
    """Return the install job's Slurm walltime when the site sets one, else None (JARVIS's)."""
    return slurm_time(timedelta(seconds=walltime_s)) if walltime_s else None


def install_pipeline(
    document: Mapping[str, object], walltime_s: int | None, answers: Mapping[str, Any]
) -> str:
    """Render the pipeline the install job builds with, from the run's ``document``.

    JARVIS sizes its own build job (``ppl install +submit``: one node, one task,
    the whole node, and its walltime from its estimate of the build). Relay
    drops its own ``pre_cmds`` (the install job serves nothing, so a package
    service's connector does not start in it) and the run's ``time``, which is
    the run's and not the build's. It gives a walltime as
    ``scheduler.install.time`` only when the site sets one (``walltime_s``) and
    the pipeline sets none; a check job (``answers`` make it a ``recheck``)
    builds nothing, so then it gets the shorter of ``walltime_s`` and
    :data:`INSTALL_CHECK_WALLTIME_S`. The person's pick when JARVIS's estimate
    was longer than the partition allows (:data:`INSTALL_SCHEDULER` in
    ``answers``) sets ``scheduler.install`` over the pipeline's own.
    """
    found = document.get("scheduler")
    if not isinstance(found, Mapping):
        return render_pipeline(document)
    scheduler = {
        key: value
        for key, value in cast(Mapping[str, object], found).items()
        if key not in ("pre_cmds", "time")
    }
    install = scheduler.get("install")
    own = dict(cast(Mapping[str, object], install)) if isinstance(install, Mapping) else {}
    chosen = answers.get(INSTALL_SCHEDULER)
    if isinstance(chosen, Mapping):
        own.update(
            {str(key): value for key, value in cast(Mapping[object, object], chosen).items()}
        )
    if walltime_s:
        seconds = min(walltime_s, INSTALL_CHECK_WALLTIME_S) if is_recheck(answers) else walltime_s
        own.setdefault("time", slurm_time(timedelta(seconds=seconds)))
    if own:
        scheduler["install"] = own
    else:
        scheduler.pop("install", None)
    return render_pipeline({**document, "scheduler": scheduler})


def install_command(
    spec: JobSpec,
    *,
    operation_id: str,
    jarvis: str,
    answers: Mapping[str, Any],
    pipeline: str | None = None,
    script: str = SCRIPT_FILE,
    home: str | None = None,
    graph: str = "",
) -> str:
    """Return the shell command that submits the install job for ``spec``'s software.

    It runs in the job directory after a submit exited 3, with the person's ``answers``
    (:func:`answers_document`). The install job is ``install_execution_id(operation_id)``
    in the operation's pipeline; ``home`` and ``graph`` are as in :func:`submit_command`.
    ``pipeline`` is the YAML of :func:`install_pipeline`, which the site script keeps as
    :data:`INSTALL_PIPELINE_FILE`; without it the install builds with the run's.

    Answers with :data:`RECHECK` set make it a ``recheck`` instead: a job in which
    JARVIS checks the software with the answers (``+check +submit``) and builds
    nothing.
    """
    recheck = is_recheck(answers)
    arguments = site_arguments(
        spec,
        mode="recheck" if recheck else "install",
        jarvis=jarvis,
        execution=install_execution_id(operation_id),
        answers={key: value for key, value in answers.items() if key not in RELAY_ANSWERS},
        pipeline=pipeline,
    )
    return _site_command(spec, arguments, script=script, home=home, graph=graph)


def bootstrap_arguments(home: str, python: str = "") -> list[str]:
    """Return :data:`BOOTSTRAP_SCRIPT`'s arguments after ``$0`` for the site home.

    ``python`` is the site's ``jarvis_python`` setting (:func:`explicit_python`).
    """
    return [home, JARVIS_PIN, JARVIS_SOURCE, python]


def direct_job(
    spec: JobSpec,
    *,
    jarvis: str = "",
    job_dir: str | None = None,
    home: str | None = None,
    site: str = "this site",
    graph: str = "",
) -> JobSpec:
    """Lower a package job to the bash job that runs it with JARVIS's direct run.

    Only for a site with no batch scheduler, where the adapter starts the job
    on the machine itself: a package never runs this way inside an allocation,
    because there ``jarvis ppl submit`` owns the job. The archive becomes an
    input, so the adapter stages it by digest; the pipeline YAML travels as an
    argument. ``job_dir`` resolves input names as in :func:`pipeline_document`,
    and ``home`` and ``graph`` are as in :func:`submit_command`. A package service is
    refused: it needs ``jarvis execution stop``, which only a submitted job has.
    """
    steps = spec.steps()
    if not steps:
        return spec
    if is_service(spec):
        raise service_refusal(site=site)
    pipeline = render_pipeline(
        pipeline_document(spec, pipeline=f"relay_{steps_digest(spec)[:12]}", job_dir=job_dir)
    )
    staged = with_archive(spec)
    data = staged.dump()
    for key in ("package", "packages", "executable", "arguments"):
        data.pop(key, None)
    data["executable"] = "/bin/bash"
    data["arguments"] = [
        "-c",
        SITE_SCRIPT,
        "clio-relay-jarvis",
        *site_arguments(spec, mode="run", jarvis=jarvis, pipeline=pipeline),
    ]
    settings = site_environment(spec, home, graph)
    if settings:
        data["environment"] = {**spec.environment, **settings}
    data["name"] = spec.name or steps[0].name.replace(".", "-")
    return JobSpec.model_validate(data)


# JARVIS's execution commands


def execution_argv(
    verb: ExecutionVerb,
    execution_id: str,
    *,
    job_dir: str,
    jarvis: str,
    pipeline: str | None = None,
    extra: Sequence[str] = (),
) -> list[str]:
    """Return the argv of ``jarvis execution <verb>`` for an execution, ``+json``.

    ``pipeline`` defaults to the operation's, ``pipeline_id(execution_id)``; the
    install job (:func:`install_execution_id`) names the operation's pipeline.
    ``cancel`` and ``stop`` print the execution record as ``get`` does. ``extra``
    are the verb's own options, such as ``move``'s ``--queue``.
    """
    if verb not in EXECUTION_VERBS:
        raise ValueError(f"unknown execution verb {verb!r}")
    return [*execution_env(job_dir), jarvis, *_execution_tail(verb, execution_id, pipeline, extra)]


def execution_env(job_dir: str) -> list[str]:
    """The ``env NAME=VALUE ...`` prefix every ``jarvis execution`` call runs under."""
    return ["env", f"JARVIS_ROOT={jarvis_root(job_dir)}", f"{NO_BUILTIN_ENV}=1"]


def _execution_tail(
    verb: ExecutionVerb, execution_id: str, pipeline: str | None, extra: Sequence[str]
) -> list[str]:
    return [
        "execution",
        verb,
        execution_id,
        *extra,
        "--pipeline-id",
        pipeline or pipeline_id(execution_id),
        "+json",
    ]


def execution_shell(
    verb: ExecutionVerb,
    execution_id: str,
    *,
    job_dir: str,
    pipeline: str | None = None,
    extra: Sequence[str] = (),
) -> str:
    """Return ``jarvis execution <verb>`` as shell, with JARVIS from ``relay.jarvis``.

    The command runs in the job directory, after ``jarvis=$(cat relay.jarvis)``.
    The env prefix is built apart from JARVIS's own argv, so neither moves the other.
    """
    if verb not in EXECUTION_VERBS:
        raise ValueError(f"unknown execution verb {verb!r}")
    env = " ".join(shlex.quote(part) for part in execution_env(job_dir))
    tail = " ".join(
        shlex.quote(part) for part in _execution_tail(verb, execution_id, pipeline, extra)
    )
    return f'{env} "$jarvis" {tail}'


def handle_pipeline(data: Mapping[str, Any]) -> tuple[str, str]:
    """Return ``(execution_id, pipeline_id)`` from a JARVIS job handle's data."""
    execution_id = str(data["execution_id"])
    return execution_id, str(data.get("pipeline_id") or pipeline_id(execution_id))


# Reading JARVIS's documents


def parse_document(text: str, schema: str) -> dict[str, Any] | None:
    """Return the last line of ``text`` that is a JSON document of ``schema``."""
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            found: object = json.loads(line)
        except ValueError:
            continue
        if isinstance(found, dict):
            document = cast(dict[str, Any], found)
            if document.get("schema_version") == schema:
                return document
    return None


def parse_handle(text: str) -> dict[str, Any] | None:
    """Return the ``jarvis.execution.handle.v1`` that ``ppl submit +json`` printed."""
    return parse_document(text, HANDLE_SCHEMA)


def handle_data(
    found: Mapping[str, Any], *, job_dir: str, shared: str | None = None
) -> tuple[str | None, dict[str, Any]]:
    """Return the native id and the job handle's data for a job ``ppl submit`` accepted.

    The data has ``mode: jarvis``, ``scheduler``, ``job_dir``, ``execution_id``,
    ``pipeline_id`` and ``execution_dir``, under ``shared`` when the site script
    named it (:func:`shared_dir`); the adapter adds its own keys.
    """
    execution_id = str(found["execution_id"])
    pipeline = str(found["pipeline_id"])
    native = found.get("scheduler_native_id")
    data: dict[str, Any] = {
        "mode": "jarvis",
        "scheduler": str(found.get("scheduler_provider") or "slurm"),
        "job_dir": job_dir,
        "execution_id": execution_id,
        "pipeline_id": pipeline,
        "execution_dir": execution_dir(job_dir, execution_id, pipeline=pipeline, shared=shared),
    }
    return (str(native) if native else None), data


_STATES = {
    "preparing": JobState.QUEUED,
    "scripted": JobState.QUEUED,
    "submitting": JobState.QUEUED,
    "submitted": JobState.QUEUED,
    "running": JobState.RUNNING,
    "completed": JobState.COMPLETED,
    "failed": JobState.FAILED,
    "canceled": JobState.CANCELLED,
}


def control_of(record: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return a record's ``cancel_request``, ``stop_request`` and ``termination``, if any."""
    metadata = _dict(record.get("metadata"))
    control = {
        key: metadata[key]
        for key in ("cancel_request", "stop_request", "termination")
        if isinstance(metadata.get(key), dict)
    }
    return control or None


def _control_message(control: Mapping[str, Any] | None, state: JobState) -> str | None:
    if not control:
        return None
    cancel = _dict(control.get("cancel_request"))
    stop = _dict(control.get("stop_request"))
    if cancel.get("status") == "failed":
        return f"jarvis execution cancel failed: {cancel.get('error') or 'no reason given'}"
    if stop.get("status") == "failed":
        return f"jarvis execution stop failed: {stop.get('error') or 'no reason given'}"
    if stop.get("status") == "fallback_cancel":
        return "the job had not started, so jarvis execution stop cancelled it"
    if state.final:
        return None
    if cancel:
        return "cancelling: jarvis execution cancel asked the scheduler to end the job"
    if stop:
        return "stopping: jarvis execution stop sent TERM, and JARVIS runs ppl stop"
    return None


def record_status(
    record: Mapping[str, Any], *, last: JobState | None = None, job_id: str | None = None
) -> JobStatus:
    """Map a ``jarvis.execution.record.v1`` onto relay's job states.

    ``unknown`` (JARVIS cannot see the scheduler's job right now) keeps ``last``,
    the state relay knew before. ``control`` carries the record's cancel and
    stop requests and how the job was terminated; an install job's progress
    comes from ``metadata.install``. ``queue`` is JARVIS's
    ``metadata.queue`` while the job waits in the queue, and ``issues`` are the
    record's typed ``metadata.issues``, and ``site_facts`` its ``metadata.site_facts``,
    both untouched. ``started_at`` is :func:`scheduler_start` for ``job_id``.
    Relay reads the scheduler only through JARVIS.
    """
    native = str(record.get("state") or "unknown")
    code = record.get("return_code")
    exit_code = code if isinstance(code, int) and not isinstance(code, bool) else None
    error = record.get("error")
    control = control_of(record)
    progress = install_progress(record)
    metadata = _dict(record.get("metadata"))
    issues = [item for item in map(_dict, cast(list[Any], metadata.get("issues") or [])) if item]
    if native == "unknown" or native not in _STATES:
        return JobStatus(
            state=last if last is not None and not last.final else JobState.RUNNING,
            native_state=native,
            message="jarvis can't see the scheduler's job right now",
            control=control,
            progress=progress,
            issues=issues,
            site_facts=_dict(metadata.get("site_facts")) or None,
            outcomes=_dict(metadata.get("outcomes")) or None,
            started_at=scheduler_start(record, job_id),
        )
    state = _STATES[native]
    queue = _dict(metadata.get("queue"))
    waiting = state == JobState.QUEUED and queue.get("state", "pending") == "pending"
    message = _control_message(control, state) or (
        error if isinstance(error, str) and error else None
    )
    if state.final and issues:
        said = "; ".join(
            dict.fromkeys(
                text
                for text in (str(item.get("message") or "").strip() for item in issues)
                if text and text != message
            )
        )
        if said and message:
            message = f"{message}{'' if message.endswith(('.', '!', '?')) else '.'} {said}"
        elif said:
            message = said
    elif waiting and queue and message is None:
        message = queue_message(queue)
    exit_code = 0 if state == JobState.COMPLETED and exit_code is None else exit_code
    return JobStatus(
        state=state,
        exit_code=exit_code if state.final else None,
        native_state=native,
        message=message,
        control=control,
        progress=progress,
        queue=queue if waiting and queue else None,
        issues=issues,
        site_facts=_dict(metadata.get("site_facts")) or None,
        outcomes=_dict(metadata.get("outcomes")) or None,
        started_at=scheduler_start(record, job_id),
    )


def _duration(seconds: int) -> str:
    hours, rest = divmod(max(seconds, 0), 3600)
    minutes = rest // 60
    if hours:
        return f"{hours} h {minutes} min" if minutes else f"{hours} h"
    return f"{minutes} min" if minutes else "under a minute"


def queue_message(queue: Mapping[str, Any]) -> str:
    """Say where a pending job stands, from JARVIS's ``jarvis.scheduler.queue.v1``."""
    partition = queue.get("partition")
    where = f" in {partition}" if isinstance(partition, str) and partition else ""
    reason = queue.get("reason")
    why = f" ({reason})" if isinstance(reason, str) and reason else ""
    starts = _int(queue.get("starts_in_s"))
    when = ""
    if starts is not None:
        when = f"; the scheduler expects it to start in about {_duration(starts)}"
    elif isinstance(queue.get("estimated_start"), str):
        when = f"; the scheduler expects it to start at {queue['estimated_start']}"
    fits = [
        str(item.get("partition"))
        for item in (_dict(raw) for raw in cast(list[Any], queue.get("partitions") or []))
        if item.get("fits") and item.get("partition") and item.get("partition") != partition
    ]
    others = f". It would fit now in {', '.join(fits)}" if fits else ""
    return f"waiting in the queue{where}{why}{when}{others}"


def _dict(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return int(value)


def artifact_location(item: Mapping[str, Any], execution: str | None) -> str | None:
    """Return where an artifact is on the site: execution paths resolve under ``execution``."""
    location = _dict(item.get("location"))
    value = location.get("value")
    if not isinstance(value, str) or not value:
        return None
    kind = location.get("kind")
    if kind == "execution_path":
        return posixpath.join(execution, value) if execution else None
    if kind == "external_uri":
        return None
    return value


def artifact_output(item: Mapping[str, Any], execution: str | None) -> OutputRef | None:
    """Return one ``jarvis.artifact.v1`` as an output, with its view descriptor."""
    path = artifact_location(item, execution)
    if path is None:
        return None
    metadata: dict[str, Any] = {
        key: item[key] for key in ("state", "kind", "role", "format", "media_type") if item.get(key)
    }
    view = _dict(_dict(item.get("metadata")).get("clio")).get("view")
    if isinstance(view, dict):
        metadata[VIEW_METADATA] = view
    return OutputRef(
        name=str(item.get("logical_name") or item.get("artifact_id")),
        path=path,
        size=_int(item.get("size_bytes")),
        metadata=metadata,
    )


def artifact_outputs(snapshot: Mapping[str, Any], *, execution: str) -> list[OutputRef]:
    """Return the outputs of a ``jarvis.execution.artifacts.v1`` snapshot."""
    items = snapshot.get("artifacts")
    outputs: dict[str, OutputRef] = {}
    for raw in cast(list[object], items) if isinstance(items, list) else []:
        output = artifact_output(_dict(raw), execution)
        if output is not None and output.name not in outputs:
            outputs[output.name] = output
    return sorted(outputs.values(), key=lambda output: output.name)


# Refusals


def refusal_document(text: str) -> dict[str, Any]:
    """Return the last ``jarvis.refusal.v1`` or failed ``jarvis.pipeline-check.v1`` in ``text``.

    A document may be on one line or pretty-printed over several; each starts a line.
    """
    decoder = json.JSONDecoder()
    for start in reversed([match.start() for match in re.finditer(r"(?m)^\s*\{", text)]):
        try:
            found, _end = decoder.raw_decode(text[start:].lstrip())
        except ValueError:
            continue
        if not isinstance(found, dict):
            continue
        document = cast(dict[str, Any], found)
        if document.get("schema_version") in (REFUSAL_SCHEMA, CHECK_SCHEMA) or (
            document.get("refused") is True or document.get("ok") is False
        ):
            return document
    return {}


def typed_issues(document: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return a refusal's ``issues``, or a failed check's ``errors``, as JARVIS typed them."""
    raw = document.get("issues")
    if not isinstance(raw, list):
        raw = document.get("errors")
    listed = cast(list[Any], raw) if isinstance(raw, list) else []
    return [cast(dict[str, Any], item) for item in listed if isinstance(item, dict)]


def jarvis_refused(text: str, *, site: str, what: str = "the pipeline") -> Refusal:
    """The refusal for a JARVIS command that exited 5 with typed issues.

    JARVIS checks the pipeline's coupling and configuration rules
    (``ppl check``) and refuses a queue move it cannot make. Relay does not
    judge these itself: it passes JARVIS's ``issues`` (``jarvis.issue.v1``:
    ``kind``, ``field``, ``issue_id``, ``message``, ``details``) on, and its
    message is theirs, so the agent sees which field to change.
    """
    document = refusal_document(text)
    issues = typed_issues(document)
    said = "; ".join(
        f"{item.get('field')}: {item.get('message')}" if item.get("field") else str(item["message"])
        for item in issues
        if item.get("message")
    )
    if not said:
        said = "\n".join(text.strip().splitlines()[-5:]) or "no output"
    return Refusal(
        RefusalReason.INVALID_SPEC,
        f"JARVIS on {site} refused {what}: {said}",
        issues=issues,
        kinds=sorted({str(item.get("kind")) for item in issues if item.get("kind")}),
        exit_code=EXIT_REFUSED,
        retry=False,
    )


def missing_jarvis(text: str, *, site: str) -> Refusal:
    """The refusal for a submit that exited 69: ``relay.refusal.json`` or stderr.

    Relay offers to install its pinned jarvis-cd when its own is missing or at
    another pin; a path the site's setting names is the site's to fix. A site
    with no Spack is JARVIS's question (``spack_missing``), not a refusal here.
    """
    written: dict[str, Any] = {}
    for line in reversed(text.splitlines()):
        try:
            found: object = json.loads(line)
        except ValueError:
            continue
        if isinstance(found, dict) and isinstance(cast(dict[str, Any], found).get("tried"), str):
            written = cast(dict[str, Any], found)
            break
    tried = str(written.get("tried") or "the site home")
    if written.get("explicit"):
        return Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"the jarvis setting of {site} names {tried}, which is not a program there. Fix "
            f"the path, or clear the setting to use relay's pinned jarvis-cd {JARVIS_PIN[:8]}",
            missing="jarvis",
            tried=tried,
            explicit=True,
        )
    pin = written.get("pin")
    if isinstance(pin, str):
        now = f"at {pin[:8]}" if pin else "unpinned"
        message = (
            f"relay's jarvis-cd on {site} ({tried}) is {now}, not {JARVIS_PIN[:8]}. Relay "
            "can install the pinned one in the site's CLIO home once you agree"
        )
        return Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            message,
            missing="jarvis",
            tried=tried,
            pin=pin,
            installable=JARVIS_PIN,
        )
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"{site} has no jarvis-cd of relay's at {tried}, so relay cannot hand the package to "
        f"JARVIS. Relay can install jarvis-cd {JARVIS_PIN[:8]} in the site's CLIO home once "
        "you agree, or the site's jarvis setting can name another by path",
        missing="jarvis",
        tried=tried,
        installable=JARVIS_PIN,
    )


def bootstrap_command(home: str, python: str = "") -> str:
    """Return the shell command that runs :data:`BOOTSTRAP_SCRIPT` for the site home.

    ``python`` is the site's ``jarvis_python`` setting, as in :func:`bootstrap_arguments`.
    """
    return shlex.join(
        ["bash", "-c", BOOTSTRAP_SCRIPT, "clio-relay-jarvis", *bootstrap_arguments(home, python)]
    )


def bootstrap_result(exit_code: int | None, output: str, *, site: str) -> str:
    """Return the ``jarvis`` path :data:`BOOTSTRAP_SCRIPT` linked, or raise its refusal."""
    if exit_code == 0:
        for line in reversed(output.splitlines()):
            try:
                found: object = json.loads(line)
            except ValueError:
                continue
            if isinstance(found, dict):
                path = cast(dict[str, Any], found).get("jarvis")
                if isinstance(path, str) and path:
                    return path
    tail = "\n".join(output.strip().splitlines()[-10:]) or "no output"
    reason = (
        RefusalReason.ADAPTER_UNAVAILABLE
        if exit_code == EXIT_NO_JARVIS
        else RefusalReason.SUBMIT_FAILED
    )
    raise Refusal(
        reason,
        f"relay could not install jarvis-cd {JARVIS_PIN[:8]} on {site}: {tail}",
        missing="jarvis",
        exit_code=exit_code,
    )


def scheduler_refusal(scheduler: str, *, site: str) -> Refusal:
    """A package job on a site whose batch scheduler JARVIS cannot submit to."""
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"a package job on {site} goes through jarvis ppl submit, and JARVIS submits to "
        f"Slurm only, not {scheduler}",
        scheduler=scheduler,
        needs=f"a JARVIS {scheduler} scheduler",
    )


def not_run_here(adapter: str) -> Refusal:
    """A package job sent to an adapter that cannot hand it to ``jarvis ppl submit``."""
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"the {adapter} adapter does not run JARVIS packages: a package job goes through "
        "jarvis ppl submit, and relay does not run a package inside an allocation itself",
        adapter=adapter,
        needs=f"jarvis ppl submit through the {adapter} adapter",
    )


def service_refusal(*, site: str) -> Refusal:
    """A package service on a site with no batch scheduler for ``jarvis ppl submit``."""
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"a package service on {site} needs jarvis ppl submit, so jarvis execution stop "
        "can end it, and the site has no batch scheduler. Give the site a Slurm scheduler",
        needs="jarvis ppl submit",
    )


def execution_failed(verb: str, text: str) -> Refusal:
    """``jarvis execution cancel|stop`` exited non-zero; ``text`` is its output."""
    tail = "\n".join(text.strip().splitlines()[-5:]) or "no output"
    return Refusal(
        RefusalReason.NOT_CANCELLABLE,
        f"jarvis execution {verb} did not end the job: {tail}",
        needs=f"jarvis execution {verb}",
    )

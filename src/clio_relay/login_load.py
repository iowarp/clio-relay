"""Keeping what relay starts on a site's login node light.

Only light, short work may run on a login node. Relay keeps two of its own
processes there, the worker's ``site-serve`` and serve's login hop, and starts
each through :func:`light_function`'s ``clio_light``: at the lowest CPU and I/O
priority (``nice -n 19``, ``ionice -c3``), inside a ``systemd-run --user
--scope`` with a CPU quota and a memory cap when the host's user manager takes
one, and under ``ulimit -v`` and ``ulimit -t`` otherwise. Each keeps one
instance per user and site (a pid file under an ``flock``) and exits on its
own: ``site-serve`` after an idle time with no request from relay, the hop
once no job it serves is left.

Jobs never run on a login node by accident: an adapter whose ``scheduler`` is
``auto`` and finds no batch scheduler refuses to run a job on the host itself
when the host still shows signs of one (:func:`batch_hints`). Only an explicit
``scheduler: none`` runs jobs there.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.jobs.refusals import Refusal, RefusalReason

BATCH_COMMANDS = ("sbatch", "squeue", "sinfo", "qsub", "qstat", "bsub", "cqsub", "flux")
"""Commands that show a host belongs to a batch system."""
BATCH_FILES = (
    "/etc/slurm/slurm.conf",
    "/etc/slurm-llnl/slurm.conf",
    "/etc/pbs.conf",
    "/var/spool/pbs",
    "/etc/lsf.conf",
)
"""Files that show the same, for hosts whose ``PATH`` lacks the commands."""
LOGIN_PROBE_TIMEOUT_S = 10


class LoginLimits(BaseModel):
    """What a long-lived relay process on a login node may use."""

    model_config = ConfigDict(extra="forbid")

    cpu_quota_percent: int = Field(
        default=25, ge=1, le=400, description="systemd-run's CPUQuota, when the host has it"
    )
    memory_mb: int = Field(
        default=512, ge=64, description="systemd-run's MemoryMax, when the host has it"
    )
    address_space_mb: int = Field(
        default=2048,
        ge=256,
        description=(
            "ulimit -v without systemd-run. It counts reserved address space, not memory in "
            "use, so it sits above memory_mb"
        ),
    )
    cpu_s: int = Field(
        default=7200,
        ge=60,
        description=(
            "ulimit -t without systemd-run: CPU seconds each process may use before it is stopped"
        ),
    )


def light_function(limits: LoginLimits | None = None) -> str:
    """Return the POSIX shell function ``clio_light``, which ``exec``s its arguments lightly.

    Run it in a subshell or as the last command: it ``exec``s, and the
    ``ulimit`` fallback limits the shell it runs in. It takes a ``systemd-run
    --user --scope`` only when the user lingers: otherwise the user manager,
    and every scope in it, ends with the person's last session.
    """
    limits = limits or LoginLimits()
    scope = (
        "systemd-run --user --scope --quiet "
        f"-p CPUQuota={limits.cpu_quota_percent}% -p MemoryMax={limits.memory_mb}M"
    )
    return (
        "clio_light() {\n"
        "  if command -v systemd-run >/dev/null 2>&1 \\\n"
        '    && [ "$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null)" = yes ] \\\n'
        f"    && {scope} true </dev/null >/dev/null 2>&1; then\n"
        f'    set -- {scope} -- "$@"\n'
        "  else\n"
        f"    ulimit -S -v {limits.address_space_mb * 1024} 2>/dev/null || :\n"
        f"    ulimit -S -t {limits.cpu_s} 2>/dev/null || :\n"
        "  fi\n"
        "  if command -v ionice >/dev/null 2>&1 && ionice -c3 true 2>/dev/null; then\n"
        '    set -- ionice -c3 "$@"\n'
        "  fi\n"
        '  if command -v nice >/dev/null 2>&1; then set -- nice -n 19 "$@"; fi\n'
        "  MALLOC_ARENA_MAX=2\n"
        "  export MALLOC_ARENA_MAX\n"
        '  exec "$@"\n'
        "}\n"
    )


def light_argv(argv: Sequence[str], limits: LoginLimits | None = None) -> list[str]:
    """Return ``argv`` wrapped in ``sh -c`` so that it runs through ``clio_light``."""
    return ["sh", "-c", light_function(limits) + 'clio_light "$@"\n', "clio-light", *argv]


BATCH_HINT_SH = (
    'if [ "$S" = none ]; then\n'
    "  h=\n"
    f"  for c in {' '.join(BATCH_COMMANDS)}; do\n"
    '    command -v "$c" >/dev/null 2>&1 && h="$h $c"\n'
    "  done\n"
    '  if [ -z "$h" ] && command -v bash >/dev/null 2>&1; then\n'
    '    t=; command -v timeout >/dev/null 2>&1 && t="timeout '
    f'{LOGIN_PROBE_TIMEOUT_S}"\n'
    '    h=$($t bash -lc \'for c in "$@"; do command -v "$c" >/dev/null 2>&1 '
    f'&& printf " %s" "$c"; done\' sh {" ".join(BATCH_COMMANDS)} </dev/null 2>/dev/null)\n'
    "  fi\n"
    f"  for f in {' '.join(BATCH_FILES)} ${{SLURM_CONF:-}}; do\n"
    '    [ -e "$f" ] && h="$h $f"\n'
    "  done\n"
    '  [ -z "$h" ] || printf "%s\\n" "@@batch_hint$h"\n'
    "fi\n"
)
"""Shell text for after ``S`` holds the detected scheduler: prints ``@@batch_hint ...``
when ``S`` is ``none`` but the host shows a batch system, in its ``PATH``, in a
login shell's ``PATH``, or in its configuration files."""


def batch_hints() -> list[str]:
    """Return what shows this host belongs to a batch system, as :data:`BATCH_HINT_SH` does."""
    hints = [command for command in BATCH_COMMANDS if shutil.which(command)]
    bash = shutil.which("bash")
    if not hints and bash is not None and os.name == "posix":
        probe = 'for c in "$@"; do command -v "$c" >/dev/null 2>&1 && printf "%s\\n" "$c"; done'
        try:
            result = subprocess.run(  # noqa: S603
                [bash, "-lc", probe, "sh", *BATCH_COMMANDS],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=LOGIN_PROBE_TIMEOUT_S,
                check=False,
            )
            hints = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        except (OSError, subprocess.SubprocessError):
            hints = []
    files = [*BATCH_FILES, *([os.environ["SLURM_CONF"]] if os.environ.get("SLURM_CONF") else [])]
    return hints + [path for path in files if os.path.exists(path)]


def direct_refused(hints: Sequence[str], *, site: str) -> Refusal:
    """Refuse to run a job on the host itself when it looks like a batch site's login node."""
    shown = ", ".join(hints)
    return Refusal(
        RefusalReason.INVALID_CONFIG,
        f"{site} shows a batch system ({shown}), but relay found no scheduler it can submit "
        "to, so it will not run the job on this host, which may be a login node. Set the "
        "site's scheduler (such as scheduler: slurm), or scheduler: none to run jobs on "
        "this host on purpose",
        batch_hints=list(hints),
        next_step="set_scheduler",
    )

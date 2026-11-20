"""The read-only ``exec`` list of a guarded site (RL24).

``exec`` on a guarded site takes an argument list, never a shell string, and
runs only commands that read: file listing and reading, scheduler queries,
module queries, quota queries, Spack queries (find, info, spec) and JARVIS
read commands. A command is checked
argument by argument; relay quotes each argument, so no character in one can
start a second command.

The program must be named bare (found on the site's ``PATH``) or exactly as
the site's ``guard.yaml`` lists it, so a file someone wrote cannot pass as
``ls`` by being called ``/tmp/x/ls``.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Sequence

PLAIN = frozenset(
    {
        "cat",
        "df",
        "du",
        "grep",
        "head",
        "hostname",
        "id",
        "ls",
        "md5sum",
        "nproc",
        "pwd",
        "qstat",
        "quota",
        "readlink",
        "realpath",
        "sacct",
        "sha256sum",
        "sinfo",
        "sprio",
        "squeue",
        "sshare",
        "stat",
        "tail",
        "uname",
        "wc",
        "whoami",
    }
)
"""Programs that only read, whatever their arguments."""

_FIND_ACTIONS = frozenset(
    {"-exec", "-execdir", "-ok", "-okdir", "-delete", "-fprint", "-fprint0", "-fprintf", "-fls"}
)
_PBSNODES_CHANGES = frozenset({"-o", "-r", "-c", "-N", "-C", "-A"})
_JARVIS_READS = {
    "ppl": frozenset({"list", "status", "print", "path"}),
    "repo": frozenset({"list"}),
    "pkg": frozenset({"readme", "help", "path"}),
    "execution": frozenset({"get", "list", "progress", "artifacts"}),
}
_MODULE_READS = frozenset(
    {"avail", "list", "spider", "show", "display", "help", "whatis", "keyword", "overview"}
)
_SRUN_OPTIONS = frozenset({"--overlap", "-N", "--nodes", "-n", "--ntasks", "-w", "--nodelist"})
_SRUN_VALUED = frozenset({"-N", "--nodes", "-n", "--ntasks", "-w", "--nodelist", "--jobid"})


def _positionals(arguments: Sequence[str]) -> list[str]:
    return [item for item in arguments if not item.startswith("-")]


def _first_positional_in(arguments: Sequence[str], allowed: Collection[str]) -> bool:
    found = _positionals(arguments)
    return bool(found) and found[0] in allowed


def _find(arguments: Sequence[str]) -> str | None:
    bad = [item for item in arguments if item in _FIND_ACTIONS]
    return f"find {bad[0]} changes or runs things" if bad else None


def _file(arguments: Sequence[str]) -> str | None:
    if any(item in ("-C", "--compile") for item in arguments):
        return "file --compile writes a magic file"
    return None


def _scontrol(arguments: Sequence[str]) -> str | None:
    options = {item for item in arguments if item.startswith("-")}
    if options - {"-d", "-o", "--details", "--oneliner"}:
        return "scontrol takes only -d and -o here"
    if not _first_positional_in(arguments, ("show",)):
        return "only `scontrol show` reads"
    return None


def _sacctmgr(arguments: Sequence[str]) -> str | None:
    options = {item for item in arguments if item.startswith("-")}
    if options - {"-n", "-p", "-P", "--noheader", "--parsable", "--parsable2"}:
        return "sacctmgr takes only output options here"
    if not _first_positional_in(arguments, ("show", "list")):
        return "only `sacctmgr show` and `sacctmgr list` read"
    return None


def _pbsnodes(arguments: Sequence[str]) -> str | None:
    bad = [item for item in arguments if item in _PBSNODES_CHANGES]
    return f"pbsnodes {bad[0]} changes a node" if bad else None


def _lfs(arguments: Sequence[str]) -> str | None:
    if not arguments or arguments[0] not in ("quota", "df", "getstripe"):
        return "only `lfs quota`, `lfs df` and `lfs getstripe` read"
    return None


def _module(arguments: Sequence[str]) -> str | None:
    if not _first_positional_in(arguments, _MODULE_READS):
        return "only module queries (avail, list, spider, show, ...) read"
    return None


def _jarvis(arguments: Sequence[str]) -> str | None:
    if len(arguments) >= 2 and arguments[1] in _JARVIS_READS.get(arguments[0], ()):
        return None
    return "only JARVIS read commands (ppl list/status/print/path, repo list, ...) read"


SPACK_READS = frozenset({"find", "info", "spec"})
"""Spack's read-only queries. None installs, builds or changes a config."""
_SPACK_GLOBAL_FLAGS = frozenset({"-E", "--no-env", "--color=never", "--color=auto"})


def spack_refusal(arguments: Sequence[str]) -> str | None:
    """Return why a Spack command does not only read, or None for find, info and spec.

    No option may name another config scope or environment (``-C``, ``-e``,
    ``-D``): a config there could add a package repository, and ``spack info``
    imports a repository's package files, which is running code.
    """
    index = 0
    while index < len(arguments) and arguments[index].startswith("-"):
        if arguments[index] not in _SPACK_GLOBAL_FLAGS:
            return f"spack {arguments[index]} is not allowed here"
        index += 1
    if index >= len(arguments) or arguments[index] not in SPACK_READS:
        return "only `spack find`, `spack info` and `spack spec` read"
    return None


RESTRICTED: dict[str, Callable[[Sequence[str]], str | None]] = {
    "file": _file,
    "find": _find,
    "lfs": _lfs,
    "module": _module,
    "pbsnodes": _pbsnodes,
    "sacctmgr": _sacctmgr,
    "scontrol": _scontrol,
}
"""Programs that read only with some arguments."""


def _srun(arguments: Sequence[str], warm_jobs: Collection[str]) -> tuple[str | None, list[str]]:
    """Split ``srun --jobid=N --overlap ...`` into a refusal or the inner command."""
    job: str | None = None
    overlap = False
    index = 0
    while index < len(arguments) and arguments[index].startswith("-"):
        option, eq, value = arguments[index].partition("=")
        if option not in _SRUN_OPTIONS and option != "--jobid":
            return f"srun {option} is not allowed here", []
        if option in _SRUN_VALUED and not eq:
            index += 1
            if index >= len(arguments):
                return f"srun {option} needs a value", []
            value = arguments[index]
        if option == "--jobid":
            job = value
        overlap = overlap or option == "--overlap"
        index += 1
    if job is None or not overlap:
        return "srun runs here only as `srun --jobid=<warm job> --overlap <command>`", []
    if job not in warm_jobs:
        return f"job {job} is not a warm allocation relay holds on this site", []
    inner = list(arguments[index:])
    if not inner:
        return "srun needs a command to run in the warm allocation", []
    return None, inner


def is_spack(program: str, spack: Collection[str] = ()) -> bool:
    """Return whether ``program`` is Spack: bare, or a Spack the site's guard lists."""
    return program == "spack" or program in spack


def runs_spack(command: Sequence[str], spack: Collection[str] = ()) -> bool:
    """Return whether ``command`` runs Spack on the login node, directly or after ``env``."""
    if not command:
        return False
    if command[0] == "env":
        _reason, inner = _env(command[1:])
        return bool(inner) and is_spack(inner[0], spack)
    return is_spack(command[0], spack)


SPACK_ENV = {"SPACK_DISABLE_LOCAL_CONFIG": frozenset({"1", "true"})}
"""What ``env NAME=VALUE spack ...`` may set before a Spack query: only dropping
the user scope, never pointing Spack at another config."""


def _env(arguments: Sequence[str]) -> tuple[str | None, list[str]]:
    """Split ``env NAME=VALUE ... spack ...`` into a refusal or the Spack command."""
    index = 0
    while index < len(arguments) and "=" in arguments[index]:
        name, _eq, value = arguments[index].partition("=")
        if value not in SPACK_ENV.get(name, ()):
            return "env may set only SPACK_DISABLE_LOCAL_CONFIG=1 here", []
        index += 1
    inner = list(arguments[index:])
    if not inner or inner[0].startswith("-"):
        return "env runs here only as `env NAME=VALUE ... spack <query>`", []
    return None, inner


def refusal(
    command: Sequence[str],
    *,
    extra: Collection[str] = (),
    jarvis: str = "jarvis",
    warm_jobs: Collection[str] = (),
    spack: Collection[str] = (),
) -> str | None:
    """Return why ``command`` may not run on a guarded site, or None when it only reads."""
    if not command:
        return "exec needs a command"
    if any("\x00" in item for item in command):
        return "an argument holds a NUL byte"
    program, arguments = command[0], list(command[1:])
    if program in extra:
        return None
    if program in ("jarvis", jarvis):
        return _jarvis(arguments)
    if is_spack(program, spack):
        return spack_refusal(arguments)
    if program == "env":
        reason, inner = _env(arguments)
        if reason is not None:
            return reason
        if not is_spack(inner[0], spack):
            return "env runs here only before a Spack query"
        return spack_refusal(inner[1:])
    if "/" in program:
        return f"name the program bare, not as {program!r}"
    if program == "srun":
        reason, inner = _srun(arguments, warm_jobs)
        if reason is not None:
            return reason
        if inner[0] == "srun":
            return "srun inside srun is not allowed"
        return refusal(inner, extra=extra, jarvis=jarvis, warm_jobs=(), spack=spack)
    if program in PLAIN:
        return None
    check = RESTRICTED.get(program)
    if check is not None:
        return check(arguments)
    return f"{program} is not on the read-only list"


def listed(extra: Collection[str] = ()) -> list[str]:
    """Return the programs the read-only list names, for a refusal's guidance."""
    return sorted({*PLAIN, *RESTRICTED, "jarvis", "spack", "srun", *extra})

"""Slurm's filename patterns in a job's stdin, stdout and stderr paths.

sbatch expands ``%j`` and its kin only in ``--input``, ``--output`` and ``--error``.
PSI/J's Slurm script points those at ``/dev/null`` and redirects the command
itself, so a path such as ``run-%j.out`` would stay literal. Relay puts a
placeholder in the launch command for such a path and, once the script is
rendered, writes the path as a shell word the batch shell expands from the
job's own environment.
"""

from __future__ import annotations

import re
import shlex
from collections.abc import Iterable

# The patterns sbatch documents for a batch script's files, less the zero-padded
# forms (``%3a``), which are left as they are.
_EXPANSIONS = {
    "%": "%",
    "j": '"${SLURM_JOB_ID}"',
    "x": '"${SLURM_JOB_NAME}"',
    "u": '"$(id -un)"',
    "N": '"${SLURMD_NODENAME}"',
    "A": '"${SLURM_ARRAY_JOB_ID:-${SLURM_JOB_ID}}"',
    "a": '"${SLURM_ARRAY_TASK_ID:-4294967294}"',
}
_PATTERN = re.compile(r"%([%jxuNAa])")


def has_patterns(path: str) -> bool:
    """Whether ``path`` holds a pattern sbatch would expand."""
    return _PATTERN.search(path) is not None


def shell_word(path: str) -> str:
    """``path`` as one shell word, with each pattern expanded when the script runs."""
    parts: list[str] = []
    literal = ""
    end = 0
    for match in _PATTERN.finditer(path):
        literal += path[end : match.start()]
        end = match.end()
        expansion = _EXPANSIONS[match.group(1)]
        if expansion == "%":
            literal += "%"
            continue
        if literal:
            parts.append(shlex.quote(literal))
            literal = ""
        parts.append(expansion)
    literal += path[end:]
    if literal:
        parts.append(shlex.quote(literal))
    return "".join(parts)


def mark(command: list[str], indices: Iterable[int]) -> dict[str, str]:
    """Swap each path at ``indices`` that holds a pattern for a placeholder.

    The placeholder is plain enough that PSI/J writes it into the script bare.
    Returns each placeholder with the path it stands for.
    """
    marks: dict[str, str] = {}
    for index in indices:
        if has_patterns(command[index]):
            placeholder = f"__relay_path_{index}__"
            marks[placeholder] = command[index]
            command[index] = placeholder
    return marks


def expand(script: str, marks: dict[str, str]) -> str:
    """Write each marked path back into ``script`` as a shell word."""
    for placeholder, path in marks.items():
        if script.count(placeholder) != 1:
            raise ValueError(f"the rendered script does not hold {placeholder} once")
        script = script.replace(placeholder, shell_word(path))
    return script

"""Dropping a job's spent inputs from the site's content store (RL16).

Inputs are hard links from the job's directory into
``<remote_dir>/content/cas/sha256``. When a job deletes an input, for example
a file of secrets it reads once, the store's copy would outlive it. The final
record of the job drops that copy unless another job still links it. An input
marked ``transient`` goes at the final state in any case, the job's copy and
the store's, even when the job never started.
"""

from __future__ import annotations

import json
import re
import shlex
from collections.abc import Iterable, Sequence
from pathlib import PurePosixPath
from typing import NamedTuple, cast

from clio_relay.jobs.spec import InputRef

_HEX = re.compile(r"[0-9a-f]{64}")


class StagedInput(NamedTuple):
    name: str
    sha256: str
    transient: bool = False


def entry(item: InputRef) -> dict[str, object]:
    """The ``inputs.json`` entry for one input."""
    return {
        "name": item.name,
        "sha256": item.sha256,
        "transient": bool(getattr(item, "transient", False)),
    }


def drop_script(root: str, inputs: Sequence[StagedInput]) -> str:
    """Shell run in the job's directory, after ``fresh`` is defined.

    A blob is removed when the input is transient, or when the job's copy is
    gone and the blob has no other link (``find -links 1``), so a job that kept,
    replaced or shares an ordinary input leaves the store alone. Other jobs'
    hard links keep their own data. Each drop prints ``@@dropped <digest>``.
    """
    body = f"R={shlex.quote(root)}\n"
    for name, digest, transient in inputs:
        shard = f'"$R"/content/cas/sha256/{shlex.quote(digest[:2])}'
        blob = f"{shard}/{shlex.quote(digest)}"
        body += f"fresh {shlex.quote(str(PurePosixPath(name).parent))} {shard}\n"
        if transient:
            body += (
                f"rm -f {shlex.quote(name)}\n"
                f'if [ -f {blob} ]; then rm -f {blob} && echo "@@dropped {digest}"; fi\n'
            )
            continue
        body += (
            f"if [ ! -e {shlex.quote(name)} ] && [ -f {blob} ] && "
            f'[ -n "$(find {blob} -links 1 2>/dev/null)" ]; then\n'
            f'  rm -f {blob} && echo "@@dropped {digest}"\nfi\n'
        )
    return body


def input_entries(lines: Iterable[str]) -> list[StagedInput]:
    """Parse the job's ``inputs.json``; nothing if unreadable."""
    try:
        items = json.loads("\n".join(lines) or "[]")
    except ValueError:
        return []
    if not isinstance(items, list):
        return []
    found: list[StagedInput] = []
    for item in cast(list[object], items):
        if isinstance(item, dict):
            fields = cast(dict[str, object], item)
            name, digest = fields.get("name"), fields.get("sha256")
            if isinstance(name, str) and isinstance(digest, str) and _HEX.fullmatch(digest):
                found.append(StagedInput(name, digest, fields.get("transient") is True))
    return found

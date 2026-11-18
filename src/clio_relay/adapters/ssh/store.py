"""Dropping a job's deleted inputs from the site's content store (RL16).

Inputs are hard links from the job's directory into
``<remote_dir>/content/cas/sha256``. When a job deletes an input, for example
a file of secrets it reads once, the store's copy would outlive it. The final
record of the job drops that copy unless another job still links it.
"""

from __future__ import annotations

import json
import re
import shlex
from collections.abc import Iterable, Sequence
from pathlib import PurePosixPath
from typing import cast

_HEX = re.compile(r"[0-9a-f]{64}")


def drop_script(root: str, inputs: Sequence[tuple[str, str]]) -> str:
    """Shell run in the job's directory, after ``fresh`` is defined.

    A blob is removed only when the job's copy is gone and the blob has no
    other link (``find -links 1``), so a job that kept, replaced or shares an
    input leaves the store alone. Each drop prints ``@@dropped <digest>``.
    """
    body = f"R={shlex.quote(root)}\n"
    for name, digest in inputs:
        shard = f'"$R"/content/cas/sha256/{shlex.quote(digest[:2])}'
        blob = f"{shard}/{shlex.quote(digest)}"
        body += (
            f"fresh {shlex.quote(str(PurePosixPath(name).parent))} {shard}\n"
            f"if [ ! -e {shlex.quote(name)} ] && [ -f {blob} ] && "
            f'[ -n "$(find {blob} -links 1 2>/dev/null)" ]; then\n'
            f'  rm -f {blob} && echo "@@dropped {digest}"\nfi\n'
        )
    return body


def input_entries(lines: Iterable[str]) -> list[tuple[str, str]]:
    """Parse the job's ``inputs.json`` into ``(name, sha256)`` pairs; nothing if unreadable."""
    try:
        items = json.loads("\n".join(lines) or "[]")
    except ValueError:
        return []
    if not isinstance(items, list):
        return []
    found: list[tuple[str, str]] = []
    for item in cast(list[object], items):
        if isinstance(item, dict):
            entry = cast(dict[str, object], item)
            name, digest = entry.get("name"), entry.get("sha256")
            if isinstance(name, str) and isinstance(digest, str) and _HEX.fullmatch(digest):
                found.append((name, digest))
    return found

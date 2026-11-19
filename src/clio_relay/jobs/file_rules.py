"""The ``files`` rules every adapter follows (docs/files.md).

Paths: an absolute path is the site's own path, ``~/`` is the site user's
home where the site has one, and a relative path is under relay's work
directory on the site and may not leave it. Jobs run in
``jobs/<operation id>`` under that work directory.

Chunked writes: a write with ``final=False``, or with ``offset > 0``, goes to
``<path>.relay-part``. Each chunk must start where the part ends; the chunk
with ``final=True`` moves the part into place. After a dropped link,
``stat("<path>.relay-part")`` says where to resume, and a chunk at the wrong
offset refuses with ``resume_at`` in its data.

Delete: a missing path refuses with ``path_not_found``, a non-empty directory
needs ``recursive``, a symlink is removed itself and never followed, and the
work directory, the home directory and ``/`` are never deleted.
"""

from __future__ import annotations

import posixpath

from clio_relay.jobs.refusals import Refusal, RefusalReason

PART_SUFFIX = ".relay-part"
CHUNK_BYTES = 4 * 1024 * 1024
"""The ``files`` verb's inline limit, and so the size of one upload chunk."""


def part_path(path: str) -> str:
    """Return where a chunked write of ``path`` collects its bytes."""
    return path.rstrip("/") + PART_SUFFIX


def check_offset(path: str, offset: int) -> None:
    """Refuse a negative offset."""
    if offset < 0:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "offset is not negative", path=path)


def check_relative(path: str) -> None:
    """Refuse a relative path that leaves the work directory."""
    if path.startswith(("/", "~")):
        return
    normal = posixpath.normpath(path.replace("\\", "/")) if path else "."
    if normal == ".." or normal.startswith("../"):
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            "a relative path must stay inside relay's work directory",
            path=path,
        )


def resume_refusal(path: str, part_size: int) -> Refusal:
    """The refusal for a chunk that does not start where the part ends."""
    if part_size == 0:
        message = f"no upload of {path} is in progress; send the first chunk at offset 0"
    else:
        message = (
            f"the upload of {path} holds {part_size} bytes; send the next chunk at "
            f"offset {part_size}"
        )
    return Refusal(RefusalReason.INVALID_ARGUMENTS, message, path=path, resume_at=part_size)


def not_empty_refusal(path: str) -> Refusal:
    """The refusal for deleting a non-empty directory without ``recursive``."""
    return Refusal(
        RefusalReason.INVALID_ARGUMENTS,
        f"{path} is a directory that is not empty; pass recursive to delete it",
        path=path,
        kind="dir",
    )


def protected_refusal(path: str) -> Refusal:
    """The refusal for deleting the work directory, the home directory or ``/``."""
    return Refusal(
        RefusalReason.INVALID_ARGUMENTS,
        f"relay does not delete {path}: it is the work directory, the home directory or /",
        path=path,
    )


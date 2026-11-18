"""SHA-256 of a file, read in chunks."""

from __future__ import annotations

import hashlib
from pathlib import Path

from clio_relay.filesystem_paths import internal_filesystem_path


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest of an artifact without loading it all at once."""
    digest = hashlib.sha256()
    with internal_filesystem_path(path, force_extended=True).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

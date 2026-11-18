"""A local content-addressed store (RL16).

Bytes live at ``<root>/cas/sha256/<first two hex>/<digest>``. Staging the same
bytes twice is a no-op, so a retried upload is never sent again.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from pathlib import Path

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import SHA256_PATTERN


class ContentStore:
    """A directory of blobs named by their SHA-256."""

    def __init__(self, root: Path) -> None:
        self.root = root / "cas" / "sha256"

    def path_for(self, digest: str) -> Path:
        """Return where ``digest`` lives, whether or not it is present."""
        digest = digest.lower()
        if not SHA256_PATTERN.match(digest):
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "not a sha256 digest", digest=digest)
        return self.root / digest[:2] / digest

    def has(self, digest: str) -> bool:
        """Return whether the store holds ``digest``."""
        return self.path_for(digest).is_file()

    def put(self, data: bytes) -> str:
        """Store ``data`` and return its digest."""
        digest = hashlib.sha256(data).hexdigest()
        target = self.path_for(digest)
        if target.is_file():
            return digest
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".blob.", dir=target.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            os.replace(tmp, target)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        return digest

    def drop(self, digest: str) -> bool:
        """Remove ``digest``. Return whether it was present."""
        try:
            self.path_for(digest).unlink()
        except FileNotFoundError:
            return False
        return True

    def get(self, digest: str) -> bytes:
        """Return the bytes of ``digest``. Refuses with ``input_missing``."""
        path = self.path_for(digest)
        try:
            return path.read_bytes()
        except FileNotFoundError as exc:
            raise Refusal(
                RefusalReason.INPUT_MISSING,
                f"content {digest} is not staged on this site",
                digest=digest,
            ) from exc

    def materialize(self, digest: str, target: Path) -> None:
        """Copy ``digest`` to ``target``. Refuses with ``input_missing``."""
        source = self.path_for(digest)
        if not source.is_file():
            raise Refusal(
                RefusalReason.INPUT_MISSING,
                f"content {digest} is not staged on this site",
                digest=digest,
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
